import os
import pandas as pd
import snowflake.connector
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# ====== SNOWFLAKE ======
SF_USER     = os.environ["SNOWFLAKE_USER"]
SF_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SF_ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "DTM")
SF_SCHEMA   = os.environ.get("SNOWFLAKE_SCHEMA", "P_STG_DTM_EHV")
SF_WAREHOUSE= os.environ.get("SNOWFLAKE_WAREHOUSE")
SF_ROLE     = os.environ.get("SNOWFLAKE_ROLE")

# ====== GMAIL ======
MAIL_FROM         = os.environ["MAIL_FROM"]        # tu gmail
MAIL_TO           = os.environ["MAIL_TO"]          # destinatario
GMAIL_APP_PASSWORD= os.environ["GMAIL_APP_PASSWORD"]

QUERY = """
WITH lic_anterior_mala AS (
    -- licencias que tienen una anterior rechazada/reducida y que termina justo 1 día antes
    SELECT DISTINCT
        curr."lcc_idn"
    FROM LCC.P_RDV_DST_LND_SYB.LCC curr
    JOIN LCC.P_RDV_DST_LND_SYB.LCC prev
      ON prev."lcc_cotrut" = curr."lcc_cotrut"               -- mismo afiliado
     AND prev."lcc_visres" IN (2, 4)                          -- rechazada o reducida
     AND DATEDIFF('day', prev."lcc_visfin", curr."lcc_fecini") = 1  -- terminó 1 día antes
     AND prev."lcc_idn" < curr."lcc_idn"                      -- es anterior
) --CTE para sacar la licencia anterior (rechazada o afiliada) Me entrega tabla de currents que cumplen con condición restrictiva


SELECT
-- ALFILDIARIO.*
alfildiario.afiliado_rut,
bl."lcc_comcod"||'-'|| bl.lcc_comcor FOLIO,
bl.lcc_operador Operador,
bl_primer_folio AS Folio_Interno,
bl."lcc_comcod",
CASE
    WHEN bl.i_visdigusu ILIKE '%autvis%' THEN 'autovis'
    ELSE alfildiario.semaforo
END AS semaforo_overwrite
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
           --  and "lcc_idn" = '20220619704'
           --  and   rn = 1
              group by "lcc_idn","lcc_visjus", "lcc_visdigfec","lcc_vismod","GLS_CAUMOD","GLS_CAUMOD2"
            ) lccrec on lccrec."lcc_idn" =  bl.BL_PRIMER_FOLIO and rnum = 1
    LEFT  JOIN  "LCC"."P_RDV_DST_LND_SYB"."LCC" lcc ON lcc."lcc_idn"= bl.BL_PRIMER_FOLIO
    left join LCC.P_RDV_DST_LND_SYB.LICCAUMOD caumod_lcc on caumod_lcc."COD_CAUMOD" = lcc."lcc_vismod"
    LEFT JOIN AFI.P_RDV_DST_LND_SYB.BEN bn
        ON bn."fld_cotrut" = bl.afiliado_rut
    LEFT JOIN AFI.P_RDV_DST_LND_SYB.CNT cn
        ON cn."fld_cotrut" = bl.afiliado_rut
    -- licencias que quiero excluir por “anterior rechazada/reducida”
    LEFT JOIN lic_anterior_mala lam
    ON lam."lcc_idn" = lcc."lcc_idn"
    WHERE 
    1=1
    AND NOT EXISTS (
        SELECT 1
        FROM LCC.P_RDV_DST_LND_SYB.ACC acc
        WHERE acc."lcc_cotrut" = bl.afiliado_rut
          AND acc."acc_idn" IN (27, 29, 42, 61, 62, 78)
    ) -- Afiliado no tiene acción
    AND semaforo_overwrite = 'VERDE' --Verdes no FT1
    AND lam."lcc_idn" IS NULL --Condición que no tenga licencia anterio rechazada o reducida
    AND TO_DATE(bl.FECHA_RECEPCION) >= DATEADD(day, IFF(dayofweek(current_date())=1,-3,-1), CURRENT_DATE()) --Logica para lunes y FDS
    AND bl.continua_calc = 'N' --Regla solo primeras 
    AND alfildiario.tipo_f_lm_cod in (1,7) --Solo licencias 1 y 7
    AND bl.diassolicitado < 21
    AND LCC_COMCOD = 3 --Solo electronicas
    and bl.med_tratante_tipo <> 'MATRONA' --Regla matronas
    AND ABS( NVL(bn."fld_benacredita", 0) + DATEDIFF('month', cn."fld_inivigfec", CURRENT_DATE()) ) > 2 --Regla acreditación
    -- AND ALFILDIARIO.diassolicitado 
    and alfildiario.lcc_medrut <> bl.afiliado_rut --Regla medico distinto a afiliado
    AND NOT (bl.CIE_GRUPO= 'PSIQUIATRICAS' and bl.diassolicitado>15) --Regla no más de 15 días siquiatricas
    AND NOT ( --Regla rechazada o no tramitada por empleador
    UPPER(lcc."lcc_observ") LIKE 'NO TRAMITADA POR%'
    OR UPPER(lcc."lcc_observ") LIKE 'RECHAZADA POR EL EMPLEADOR%')
    -- AND "lcc_estado" is not null
AND EXISTS (
    SELECT 1
    FROM AFI.P_RDV_DST_LND_SYB.CNT CTNN
    WHERE CTNN."fld_cotrut" = lcc."lcc_cotrut"
      AND lcc."lcc_fecemi" BETWEEN CTNN."fld_inivigfec" AND CTNN."fld_finvigfec"
) -- Regla que el contrato esté vigente
AND NOT EXISTS (
    SELECT 1
    FROM LCC.P_RDV_DST_LND_SYB.LCC lcc2
    WHERE 1=1
      -- mismo afiliado
      AND lcc2."lcc_cotrut" = lcc."lcc_cotrut"
      -- si también quieres mismo empleador, descomenta esta:
      -- AND lcc2."lcc_emprut" = lcc."lcc_emprut"
      -- que no sea la misma licencia
      AND lcc2."lcc_idn" <> lcc."lcc_idn"
      -- solo licencias emitidas antes o el mismo día
      AND lcc2."lcc_fecemi" <= lcc."lcc_fecemi"
      -- que no estén rechazadas / en estados que no valen
      AND lcc2."lcc_visres" <> 2
      AND lcc2."lcc_estado" NOT IN (8, 12, 13, 15)
      -- y AQUÍ el solape de rangos:
      AND (
            lcc."lcc_visini" BETWEEN lcc2."lcc_visini" AND lcc2."lcc_visfin"
         OR lcc."lcc_visfin" BETWEEN lcc2."lcc_visini" AND lcc2."lcc_visfin"
         OR lcc2."lcc_visini" BETWEEN lcc."lcc_visini" AND lcc."lcc_visfin"
         OR lcc2."lcc_visfin" BETWEEN lcc."lcc_visini" AND lcc."lcc_visfin"
      )  
) -- Regla supersición
AND ( -- Regla cies días máx
    -- 1) si el CIE no está en los que tienen tope, lo dejamos pasar
    bl.cie_f NOT IN (
        'A09','B30','E02','E03','E04','E05','E07','E11',
        'F31','F32','F33','F34','F41','F42','F43','F44','F45','F48','F51',
        'G43','G56','H81',
        'J00','J01','J02','J03','J06','J11','J12','J15','J16','J17','J18','J20','J45','J46',
        'K58',
        'L03',
        'M15','M16','M17','M50','M53','M54','M65','M75','M77','M79',
        'N93',
        'R10',
        'S03','S93'
    )
    -- 2) si está en la lista, solo pasa si no se pasó del máximo
    OR (
        (bl.cie_f = 'A09' AND bl.diassolicitado <= 5) OR
        (bl.cie_f = 'B30' AND bl.diassolicitado <= 5) OR
        (bl.cie_f = 'E02' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'E03' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'E04' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'E05' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'E07' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'E11' AND bl.diassolicitado <= 11) OR

        (bl.cie_f = 'F31' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F32' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F33' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F34' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F41' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F42' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F43' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F44' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F45' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F48' AND bl.diassolicitado <= 14) OR
        (bl.cie_f = 'F51' AND bl.diassolicitado <= 14) OR

        (bl.cie_f = 'G43' AND bl.diassolicitado <= 5) OR
        (bl.cie_f = 'G56' AND bl.diassolicitado <= 7) OR

        (bl.cie_f = 'H81' AND bl.diassolicitado <= 11) OR

        (bl.cie_f = 'J00' AND bl.diassolicitado <= 3) OR
        (bl.cie_f = 'J01' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'J02' AND bl.diassolicitado <= 3) OR
        (bl.cie_f = 'J03' AND bl.diassolicitado <= 5) OR
        (bl.cie_f = 'J06' AND bl.diassolicitado <= 5) OR
        (bl.cie_f = 'J11' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'J12' AND bl.diassolicitado <= 10) OR
        (bl.cie_f = 'J15' AND bl.diassolicitado <= 10) OR
        (bl.cie_f = 'J16' AND bl.diassolicitado <= 10) OR
        (bl.cie_f = 'J17' AND bl.diassolicitado <= 10) OR
        (bl.cie_f = 'J18' AND bl.diassolicitado <= 10) OR
        (bl.cie_f = 'J20' AND bl.diassolicitado <= 5) OR
        (bl.cie_f = 'J45' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'J46' AND bl.diassolicitado <= 7) OR

        (bl.cie_f = 'K58' AND bl.diassolicitado <= 3) OR

        (bl.cie_f = 'L03' AND bl.diassolicitado <= 7) OR

        (bl.cie_f = 'M15' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'M16' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'M17' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'M50' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'M53' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'M54' AND bl.diassolicitado <= 5) OR
        (bl.cie_f = 'M65' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'M75' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'M77' AND bl.diassolicitado <= 7) OR
        (bl.cie_f = 'M79' AND bl.diassolicitado <= 7) OR

        (bl.cie_f = 'N93' AND bl.diassolicitado <= 7) OR

        (bl.cie_f = 'R10' AND bl.diassolicitado <= 5) OR

        (bl.cie_f = 'S03' AND bl.diassolicitado <= 15) OR
        (bl.cie_f = 'S93' AND bl.diassolicitado <= 15)
    )
);
"""

def connect_to_snowflake():
    print("Conectando a Snowflake...")
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE,
    )
    print("✅ Conexión a Snowflake OK")
    return conn

def fetch_dataframe(sql: str) -> pd.DataFrame:
    conn = connect_to_snowflake()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

def save_excel(df: pd.DataFrame, path: str):
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="datos")

def send_email_gmail(subject: str, content_text: str, attachment_path: str = None):
    smtp_server = "smtp.gmail.com"
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
        server.login(MAIL_FROM, GMAIL_APP_PASSWORD)
        server.send_message(msg)

    print("📧 Correo enviado por Gmail.")

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
    send_email_gmail(subject, body, excel_name)

if __name__ == "__main__":
    main()

