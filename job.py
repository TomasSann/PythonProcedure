import os
import pandas as pd
import snowflake.connector
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


# =========================
#  CONFIG DESDE SECRETS
# =========================
SF_USER     = os.environ["SNOWFLAKE_USER"]
SF_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SF_ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]      # ej: "isapre_colmena.us-east-1"
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "DTM")
SF_SCHEMA   = os.environ.get("SNOWFLAKE_SCHEMA", "P_STG_DTM_EHV")
SF_WAREHOUSE= os.environ.get("SNOWFLAKE_WAREHOUSE")   # opcional
SF_ROLE     = os.environ.get("SNOWFLAKE_ROLE")        # opcional

MAIL_FROM   = os.environ["MAIL_FROM"]  # tu correo outlook
MAIL_TO     = os.environ["MAIL_TO"]    # destino
OUTLOOK_APP_PASSWORD = os.environ["OUTLOOK_APP_PASSWORD"]  # contraseña de app outlook


# =========================
#  TU QUERY A SNOWFLAKE
# =========================
QUERY = """
SELECT
ALFILDIARIO.*,
bl.i_resolucion,
bl.i_diasatutorizados,
1-((i_diasatutorizados) / alfildiario.diassolicitado)  as tasa_rechazo_inicial,
ifnull(lccrec."GLS_CAUMOD",caumod_lcc."GLS_CAUMOD")    as rec_GLS_CAUMOD,
CASE
WHEN I_RESOLUCION in (1,3) THEN 'AUTORIZADA'
WHEN I_RESOLUCION = (4) THEN 'REDUCIDA'
WHEN I_RESOLUCION = (2) THEN 'RECHAZADA'
WHEN I_RESOLUCION = (0) THEN ' '
END AS RESOLUCION_PRIMERA,
alfildiario.diassolicitado - bl.i_diasatutorizados DIASRECHAZADOSCONTRALOR,
CASE
        WHEN alfildiario.diassolicitado <= 3 
            THEN DIASRECHAZADOSCONTRALOR * 17435
        WHEN alfildiario.diassolicitado BETWEEN 4 AND 10 
            THEN DIASRECHAZADOSCONTRALOR * 43944
        WHEN alfildiario.diassolicitado >= 11 
            THEN DIASRECHAZADOSCONTRALOR * 64099
END AS VALORIZACION_FAKE,
bl.continua_calc CONTINUA_CALC,
bl.i_visdigusu Usuariovisacion,
CASE
WHEN alfildiario.diassolicitado > 3 THEN 1
ELSE 0
END AS MAYORA3DIAS,
CASE
    WHEN bl.i_visdigusu ILIKE '%autvis%' THEN 'autovis'
    ELSE alfildiario.semaforo
END AS semaforo_overwrite,
ROUND(
    CASE 
        WHEN semaforo_overwrite = 'VERDE' 
            THEN 0
        ELSE alfildiario.diassolicitado * (1 - alfildiario.probabilidad_aprobacion)
    END
, 0) AS DIASRECHAZADOSMODELO,
ROUND(alfildiario.probabilidad_aprobacion, 3) AS prob_aprobacion_redondeada,
DIASRECHAZADOSMODELO-DIASRECHAZADOSCONTRALOR AS DIFERENCIADIAS,
CASE
        WHEN alfildiario.diassolicitado <= 3 THEN DIFERENCIADIAS * 17435
        WHEN alfildiario.diassolicitado BETWEEN 4 AND 10 THEN DIFERENCIADIAS * 43944
        ELSE DIFERENCIADIAS * 64099
END AS Valorizacion,
CASE
    WHEN MAYORA3DIAS = 1 THEN 2643
    ELSE 250
END AS Costomanualidad,
CASE
    WHEN alfildiario.diassolicitado < 4 
         AND RESOLUCION_PRIMERA <> 'AUTORIZADA'
         THEN 'Error Tipo I'
    WHEN alfildiario.diassolicitado >= 4 
         AND alfildiario.diassolicitado <= 10
         AND RESOLUCION_PRIMERA <> 'AUTORIZADA'
         THEN 'Error Tipo II'
    WHEN alfildiario.diassolicitado > 10
         AND RESOLUCION_PRIMERA <> 'AUTORIZADA'
         THEN 'Error Tipo III'
    WHEN RESOLUCION_PRIMERA = 'AUTORIZADA'
         THEN 'No hay error'
END AS TipoError
from
OPX.P_DDV_OPX_MDPREDICTIVO.FT30_PREDICCIONES_DIARIAS ALFILDIARIO
left join OPX.P_DESA_OPX.CPA_LM_BASE_AMPLIADA bl on bl.lcc_comcor||bl.afiliado_rut = ALFILDIARIO.lcc_comcor||ALFILDIARIO.afiliado_rut
left join ( 
            select
            "lcc_idn",
            "lcc_visjus",
            "lcc_vismod",
            "GLS_CAUMOD",
            "GLS_CAUMOD2",
            row_number() over (partition by "lcc_idn" order by "lcc_visdigfec" asc) as rnum           
            from LCC.P_RDV_DST_LND_SYB.LCCREC
            left join LCC.P_RDV_DST_LND_SYB.LICCAUMOD caumod on caumod."COD_CAUMOD" = "lcc_vismod"
            where 1=1
              group by "lcc_idn","lcc_visjus", "lcc_visdigfec","lcc_vismod","GLS_CAUMOD","GLS_CAUMOD2"
            ) lccrec on lccrec."lcc_idn" =  bl.BL_PRIMER_FOLIO and rnum = 1
    LEFT  JOIN  "LCC"."P_RDV_DST_LND_SYB"."LCC" lcc ON lcc."lcc_idn"= bl.BL_PRIMER_FOLIO
    left join LCC.P_RDV_DST_LND_SYB.LICCAUMOD caumod_lcc on caumod_lcc."COD_CAUMOD" = lcc."lcc_vismod"
    WHERE 
    1=1
;
"""


# =========================
#  FUNCIONES
# =========================
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE,
    )
    return conn


def fetch_dataframe(sql: str) -> pd.DataFrame:
    conn = connect_to_snowflake()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)
        return df
    finally:
        try:
            cur.close()
        except:
            pass
        conn.close()


def save_excel(df: pd.DataFrame, path: str):
    # usamos XlsxWriter para que funcione bien en GitHub Actions
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="datos")


def send_email_outlook(subject: str, content_text: str, attachment_path: str = None):
    smtp_server = "smtp.office365.com"
    smtp_port = 587

    msg = MIMEMultipart()
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    msg["Subject"] = subject

    msg.attach(MIMEText(content_text, "plain"))

    if attachment_path:
        with open(attachment_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
        part["Content-Disposition"] = f'attachment; filename="{os.path.basename(attachment_path)}"'
        msg.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(MAIL_FROM, OUTLOOK_APP_PASSWORD)
        server.send_message(msg)

    print("Correo enviado exitosamente desde Outlook.")


def main():
    print("Descargando datos de Snowflake…")
    df = fetch_dataframe(QUERY)
    print(f"Filas obtenidas: {len(df)}")

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    excel_name = f"reporte_{ts}.xlsx"
    save_excel(df, excel_name)
    print(f"Excel generado: {excel_name}")

    subject = f"Reporte Snowflake {ts}"
    body = f"Adjunto el reporte generado automáticamente. Total filas: {len(df)}."
    send_email_outlook(subject, body, excel_name)


if __name__ == "__main__":
    main()
