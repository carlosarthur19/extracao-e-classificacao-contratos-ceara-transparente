"""
================================================================================
DAG: anomalias_contratos_ceara_enriquecida
Autor base: Carlos Arthur
Evolução: ETL + ML + Classificação LLM + Relatório HTML
================================================================================

Fluxo:
  0. criar_tabelas
  1. extrair_contratos
  2. salvar_postgres
  3. detectar_anomalias
  4. salvar_anomalias
  5. classificar_anomalias_llm
  6. salvar_classificacoes_llm
  7. gerar_relatorio_consolidado
================================================================================
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import requests
from openai import OpenAI
from psycopg2.extras import execute_values
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "host.docker.internal"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "database": os.getenv("DB_NAME", "banco"),
    "user": os.getenv("DB_USER", "usuario"),
    "password": os.getenv("DB_PASSWORD", "senha"),
}

API_BASE_URL = (
    "https://api-dados-abertos.cearatransparente.ce.gov.br"
    "/transparencia/contratos/contratos"
)

DIAS_RETROATIVOS = 365
PERIODO_ANALISE_DIAS = 365
CONTAMINACAO = 0.05

# LLM
LLM_PROVIDER = Variable.get("LLM_PROVIDER", default_var="openai").lower()
OPENAI_MODEL = Variable.get("OPENAI_MODEL", default_var="gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "sua_chave_openai")
BATCH_SIZE = 10

CATEGORIAS = [
    "Saúde",
    "Educação",
    "Tecnologia da Informação",
    "Infraestrutura",
    "Segurança Pública",
    "Meio Ambiente",
    "Administrativo",
    "Outros",
]

# =============================================================================
# HELPERS
# =============================================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_dag_dir():
    return Path(__file__).resolve().parent


def get_temp_file_path(prefix: str, suffix: str):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return get_dag_dir() / f"{prefix}_{timestamp}.{suffix}"


def formatar_moeda_br(valor):
    if valor is None:
        valor = 0
    return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def parse_data(valor):
    if not valor:
        return None

    s = str(valor)
    if "T" in s:
        s = s.split("T")[0]

    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            d = datetime.strptime(s, fmt).date()
            if d.year < 1900:
                return None
            return d
        except ValueError:
            continue
    return None


def parse_valor(v):
    if v is None:
        return None
    try:
        return float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
    except (ValueError, AttributeError):
        return None


def salvar_json_em_arquivo(dados, prefixo):
    caminho = get_temp_file_path(prefixo, "json")
    with caminho.open("w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, default=str)
    return str(caminho)


def ler_json_de_arquivo(caminho):
    with Path(caminho).open("r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# TASK 0 - CRIAR TABELAS
# =============================================================================

def criar_tabelas():
    ddl_contratos = """
        CREATE TABLE IF NOT EXISTS contratos (
            id BIGSERIAL PRIMARY KEY,
            isn_sic TEXT,
            objeto TEXT,
            fornecedor_nome TEXT,
            fornecedor_cnpj TEXT,
            orgao_nome TEXT,
            modalidade TEXT,
            valor_inicial NUMERIC(18,2),
            valor_global NUMERIC(18,2),
            data_assinatura DATE,
            data_inicio_vigencia DATE,
            data_fim_vigencia DATE,
            prazo_vigencia_dias INTEGER,
            json_original JSONB,
            inserido_em TIMESTAMP DEFAULT NOW(),
            UNIQUE(isn_sic, data_assinatura)
        );
    """

    ddl_anomalias = """
        CREATE TABLE IF NOT EXISTS anomalias_contratos (
            id BIGSERIAL PRIMARY KEY,
            isn_sic TEXT,
            objeto TEXT,
            fornecedor_nome TEXT,
            orgao_nome TEXT,
            valor_global NUMERIC(18,2),
            prazo_vigencia_dias INTEGER,
            score_anomalia NUMERIC(18,6),
            percentil_risco INTEGER,
            nivel_risco TEXT,
            data_assinatura DATE,
            inserido_em TIMESTAMP DEFAULT NOW()
        );
    """

    ddl_classificadas = """
        CREATE TABLE IF NOT EXISTS anomalias_classificadas (
            id BIGSERIAL PRIMARY KEY,
            isn_sic TEXT,
            objeto TEXT,
            fornecedor_nome TEXT,
            orgao_nome TEXT,
            valor_global NUMERIC(18,2),
            prazo_vigencia_dias INTEGER,
            score_anomalia NUMERIC(18,6),
            percentil_risco INTEGER,
            nivel_risco TEXT,
            data_assinatura DATE,
            categoria TEXT,
            confianca TEXT,
            objeto_vago BOOLEAN,
            justificativa_vago TEXT,
            resumo TEXT,
            tokens_usados INTEGER,
            modelo_usado TEXT,
            provider_llm TEXT,
            classificado_em TIMESTAMP DEFAULT NOW()
        );
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_contratos)
            cur.execute(ddl_anomalias)
            cur.execute(ddl_classificadas)
        conn.commit()

    logger.info("Tabelas verificadas/criadas com sucesso.")
    return True


# =============================================================================
# TASK 1 - EXTRAÇÃO
# =============================================================================

def extrair_contratos(**context):
    data_fim = "01/01/2021"
    data_inicio = "31/12/2025"

    logger.info("Buscando contratos de %s até %s", data_inicio, data_fim)

    arquivo_temporario = get_temp_file_path("contratos_tmp", "jsonl")
    total_contratos = 0
    pagina_atual = 1
    total_paginas = None

    with arquivo_temporario.open("w", encoding="utf-8") as arquivo_saida:
        while True:
            params = {
                "page": pagina_atual,
                "data_assinatura_inicio": data_inicio,
                "data_assinatura_fim": data_fim,
            }

            try:
                response = requests.get(API_BASE_URL, params=params, timeout=30)
                response.raise_for_status()
            except requests.exceptions.Timeout:
                logger.error("Timeout na página %s. Encerrando extração.", pagina_atual)
                break
            except requests.exceptions.HTTPError as e:
                logger.error(
                    "Erro HTTP %s na página %s: %s",
                    e.response.status_code,
                    pagina_atual,
                    e,
                )
                break

            dados = response.json()
            registros = dados.get("data", [])
            meta = dados.get("sumary", {})

            if total_paginas is None:
                total_paginas = meta.get("total_pages", 1)
                total_registros = meta.get("total_records", 0)
                logger.info(
                    "Total de registros: %s | Total de páginas: %s",
                    total_registros,
                    total_paginas,
                )

            for registro in registros:
                arquivo_saida.write(json.dumps(registro, ensure_ascii=False) + "\n")

            total_contratos += len(registros)
            logger.info(
                "Página %s/%s — %s registros coletados",
                pagina_atual,
                total_paginas,
                len(registros),
            )

            if pagina_atual >= total_paginas or not registros:
                break

            pagina_atual += 1

    logger.info(
        "Extração concluída: %s contratos. Arquivo temporário: %s",
        total_contratos,
        arquivo_temporario,
    )

    context["ti"].xcom_push(key="contratos_arquivo", value=str(arquivo_temporario))
    return total_contratos


# =============================================================================
# TASK 2 - SALVAR NO POSTGRES
# =============================================================================

def salvar_postgres(**context):
    ti = context["ti"]
    arquivo_temporario = ti.xcom_pull(task_ids="extrair_contratos", key="contratos_arquivo")

    if not arquivo_temporario:
        logger.warning("Nenhum arquivo temporário recebido para salvar.")
        return 0

    caminho_arquivo = Path(arquivo_temporario)

    if not caminho_arquivo.exists():
        logger.warning("Arquivo temporário não encontrado: %s", caminho_arquivo)
        return 0

    registros = []

    try:
        with caminho_arquivo.open("r", encoding="utf-8") as arquivo_entrada:
            for linha in arquivo_entrada:
                linha = linha.strip()
                if not linha:
                    continue

                c = json.loads(linha)

                data_assinatura = parse_data(c.get("data_assinatura"))
                data_inicio = parse_data(c.get("data_inicio"))
                data_fim = parse_data(c.get("data_termino"))

                prazo_dias = None
                if data_inicio and data_fim and data_fim > data_inicio:
                    prazo_dias = (data_fim - data_inicio).days

                valor_inicial = parse_valor(c.get("valor_contrato"))
                valor_global = parse_valor(
                    c.get("valor_atualizado_concedente") or c.get("valor_contrato")
                )

                registros.append((
                    c.get("isn_sic"),
                    c.get("descricao_objeto"),
                    c.get("descricao_nome_credor"),
                    c.get("plain_cpf_cnpj_financiador") or c.get("cpf_cnpj_financiador"),
                    c.get("cod_orgao"),
                    c.get("descricao_modalidade"),
                    valor_inicial,
                    valor_global,
                    data_assinatura,
                    data_inicio,
                    data_fim,
                    prazo_dias,
                    json.dumps(c, ensure_ascii=False),
                ))

        if not registros:
            logger.warning("Arquivo temporário sem contratos para salvar.")
            return 0

        sql = """
            INSERT INTO contratos (
                isn_sic, objeto, fornecedor_nome, fornecedor_cnpj,
                orgao_nome, modalidade, valor_inicial, valor_global,
                data_assinatura, data_inicio_vigencia, data_fim_vigencia,
                prazo_vigencia_dias, json_original
            ) VALUES %s
            ON CONFLICT (isn_sic, data_assinatura) DO NOTHING
        """

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, registros, page_size=500)
                inseridos = cur.rowcount
            conn.commit()

        logger.info(
            "%s contratos processados | %s novos inseridos no PostgreSQL.",
            len(registros),
            inseridos,
        )
        return inseridos

    finally:
        if caminho_arquivo.exists():
            caminho_arquivo.unlink()
            logger.info("Arquivo temporário removido: %s", caminho_arquivo)


# =============================================================================
# TASK 3 - DETECÇÃO DE ANOMALIAS
# =============================================================================

def detectar_anomalias(**context):
    logger.info("Carregando contratos do PostgreSQL para detecção de anomalias...")

    sql_leitura = f"""
        SELECT
            id,
            isn_sic,
            objeto,
            fornecedor_nome,
            orgao_nome,
            valor_global,
            valor_inicial,
            prazo_vigencia_dias,
            data_assinatura,
            modalidade
        FROM contratos
        WHERE
            data_assinatura BETWEEN '2021-01-01' AND '2025-12-31'
            AND valor_global IS NOT NULL
            AND valor_global > 0
        ORDER BY data_assinatura DESC
    """

    with get_db_connection() as conn:
        df = pd.read_sql(sql_leitura, conn)

    logger.info("Contratos carregados para análise: %s registros", len(df))

    if len(df) < 10:
        logger.warning("Poucos contratos para treinar o modelo.")
        return 0

    df["valor_por_dia"] = df.apply(
        lambda row: row["valor_global"] / row["prazo_vigencia_dias"]
        if row["prazo_vigencia_dias"] and row["prazo_vigencia_dias"] > 0
        else row["valor_global"],
        axis=1,
    )

    df["log_valor_global"] = np.log1p(df["valor_global"])
    df["log_valor_por_dia"] = np.log1p(df["valor_por_dia"])

    features_modelo = [
        "log_valor_global",
        "log_valor_por_dia",
        "prazo_vigencia_dias",
    ]

    df_modelo = df[features_modelo + ["id", "isn_sic"]].dropna()
    X = df_modelo[features_modelo].values

    logger.info(
        "Features: %s | Contratos válidos para o modelo: %s",
        features_modelo,
        len(X),
    )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    modelo = IsolationForest(
        n_estimators=200,
        contamination=CONTAMINACAO,
        random_state=42,
        n_jobs=-1,
    )
    modelo.fit(X_scaled)

    df_modelo = df_modelo.copy()
    df_modelo["predicao"] = modelo.predict(X_scaled)
    df_modelo["score_anomalia"] = modelo.score_samples(X_scaled)

    df_resultado = df_modelo.merge(
        df[[
            "id",
            "objeto",
            "fornecedor_nome",
            "orgao_nome",
            "valor_global",
            "data_assinatura",
            "modalidade",
        ]],
        on="id",
        how="left",
    )

    df_anomalias = df_resultado[df_resultado["predicao"] == -1].copy()

    scores_invertidos = -df_anomalias["score_anomalia"]
    df_anomalias["percentil_risco"] = (
        scores_invertidos.rank(pct=True) * 100
    ).astype(int)

    def classificar_risco(percentil):
        if percentil >= 90:
            return "ALTO"
        elif percentil >= 70:
            return "MÉDIO"
        return "BAIXO"

    df_anomalias["nivel_risco"] = df_anomalias["percentil_risco"].apply(classificar_risco)

    qtd_anomalias = len(df_anomalias)
    logger.info(
        "Anomalias detectadas: %s de %s contratos analisados (%.1f%%)",
        qtd_anomalias,
        len(df_modelo),
        (qtd_anomalias / len(df_modelo) * 100) if len(df_modelo) else 0,
    )

    anomalias = df_anomalias.to_dict(orient="records")
    caminho_json = salvar_json_em_arquivo(anomalias, "anomalias_tmp")

    context["ti"].xcom_push(key="anomalias_arquivo", value=caminho_json)
    return qtd_anomalias


# =============================================================================
# TASK 4 - SALVAR ANOMALIAS
# =============================================================================

def salvar_anomalias(**context):
    ti = context["ti"]
    caminho_json = ti.xcom_pull(task_ids="detectar_anomalias", key="anomalias_arquivo")

    if not caminho_json:
        logger.info("Nenhuma anomalia para salvar.")
        return 0

    anomalias = ler_json_de_arquivo(caminho_json)

    if not anomalias:
        logger.info("Arquivo de anomalias vazio.")
        return 0

    registros = [
        (
            a.get("isn_sic"),
            a.get("objeto"),
            a.get("fornecedor_nome"),
            a.get("orgao_nome"),
            a.get("valor_global"),
            a.get("prazo_vigencia_dias"),
            float(a.get("score_anomalia", 0)),
            int(a.get("percentil_risco", 0)),
            a.get("nivel_risco"),
            a.get("data_assinatura"),
        )
        for a in anomalias
    ]

    sql_truncate = "TRUNCATE TABLE anomalias_contratos RESTART IDENTITY;"
    sql_insert = """
        INSERT INTO anomalias_contratos (
            isn_sic, objeto, fornecedor_nome, orgao_nome,
            valor_global, prazo_vigencia_dias, score_anomalia,
            percentil_risco, nivel_risco, data_assinatura
        ) VALUES %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_truncate)
            execute_values(cur, sql_insert, registros)
        conn.commit()

    df = pd.DataFrame(anomalias)
    resumo = df["nivel_risco"].value_counts().to_dict() if "nivel_risco" in df.columns else {}

    logger.info(
        "%s anomalias salvas. Resumo por risco: ALTO=%s, MÉDIO=%s, BAIXO=%s",
        len(registros),
        resumo.get("ALTO", 0),
        resumo.get("MÉDIO", 0),
        resumo.get("BAIXO", 0),
    )

    return len(registros)


# =============================================================================
# HELPERS LLM
# =============================================================================

def _montar_prompt_llm(objeto, categorias):
    cats_formatadas = "\n".join(f"  - {c}" for c in categorias)

    prompt_sistema = f"""Você é um especialista em transparência pública, contratos e compras governamentais brasileiras.

Sua tarefa é analisar o objeto de um contrato anômalo e retornar APENAS um JSON com a estrutura:

{{
  "categoria": "uma das categorias listadas abaixo",
  "confianca": "ALTA | MÉDIA | BAIXA",
  "objeto_vago": true | false,
  "justificativa_vago": "explique por que é vago; se não for, retorne string vazia",
  "resumo": "resumo simples em uma frase"
}}

CATEGORIAS DISPONÍVEIS:
{cats_formatadas}

CRITÉRIOS PARA objeto_vago=true:
- objeto genérico demais
- ausência do que exatamente será comprado/contratado
- uso de expressões vagas como "conforme necessidade"
- descrição com menos de 5 palavras informativas

REGRAS:
- responda APENAS com JSON válido
- categoria deve ser uma das opções fornecidas
- se estiver em dúvida, use "Outros"
- resumo com no máximo 15 palavras
- confiança BAIXA quando o objeto for muito vago
"""

    prompt_usuario = f'Analise este objeto de contrato:\n\n"{objeto}"'
    return prompt_sistema, prompt_usuario


def _chamar_openai(prompt_sistema, prompt_usuario):
    if not OPENAI_API_KEY:
        raise ValueError(
            "OPENAI_API_KEY não encontrada. Defina a variável de ambiente no ambiente do Airflow."
        )

    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": prompt_sistema},
            {"role": "user", "content": prompt_usuario},
        ],
        temperature=0,
        max_tokens=300,
        response_format={"type": "json_object"},
    )

    return {
        "conteudo": response.choices[0].message.content,
        "tokens": response.usage.total_tokens,
        "modelo": OPENAI_MODEL,
    }


def _classificar_objeto_com_llm(objeto):
    if LLM_PROVIDER != "openai":
        raise ValueError(
            f"LLM_PROVIDER='{LLM_PROVIDER}' ainda não implementado. Use 'openai'."
        )

    prompt_sis, prompt_usr = _montar_prompt_llm(objeto, CATEGORIAS)
    resultado = _chamar_openai(prompt_sis, prompt_usr)

    match = re.search(r"\{.*\}", resultado["conteudo"].strip(), re.DOTALL)
    if not match:
        raise ValueError(f"Nenhum JSON encontrado na resposta: {resultado['conteudo'][:120]}")

    dados = json.loads(match.group())

    categoria = dados.get("categoria", "Outros")
    if categoria not in CATEGORIAS:
        categoria = "Outros"

    confianca = dados.get("confianca", "BAIXA").upper().replace("MEDIA", "MÉDIA")
    if confianca not in ("ALTA", "MÉDIA", "BAIXA"):
        confianca = "BAIXA"

    return {
        "categoria": categoria,
        "confianca": confianca,
        "objeto_vago": bool(dados.get("objeto_vago", False)),
        "justificativa_vago": dados.get("justificativa_vago", "") or "",
        "resumo": dados.get("resumo", "") or "",
        "tokens_usados": resultado["tokens"],
        "modelo_usado": resultado["modelo"],
        "provider_llm": LLM_PROVIDER,
    }


# =============================================================================
# TASK 5 - CLASSIFICAR ANOMALIAS COM LLM
# =============================================================================

def classificar_anomalias_llm(**context):
    ti = context["ti"]
    caminho_json = ti.xcom_pull(task_ids="detectar_anomalias", key="anomalias_arquivo")

    if not caminho_json:
        logger.info("Nenhum arquivo de anomalias recebido para classificação.")
        return 0

    anomalias = ler_json_de_arquivo(caminho_json)
    if not anomalias:
        logger.info("Sem anomalias para classificar.")
        return 0

    logger.info(
        "Classificando %s contratos anômalos | Provider=%s | Modelo=%s",
        len(anomalias),
        LLM_PROVIDER,
        OPENAI_MODEL,
    )

    classificadas = []
    erros = 0
    tokens_total = 0

    for i, item in enumerate(anomalias):
        objeto = (item.get("objeto") or "").strip()

        if not objeto or len(objeto) < 5:
            classificadas.append({
                **item,
                "categoria": "Outros",
                "confianca": "BAIXA",
                "objeto_vago": True,
                "justificativa_vago": "Objeto ausente ou curto demais para classificar.",
                "resumo": "Objeto insuficiente para classificação.",
                "tokens_usados": 0,
                "modelo_usado": OPENAI_MODEL,
                "provider_llm": LLM_PROVIDER,
            })
            continue

        try:
            resultado = _classificar_objeto_com_llm(objeto)
            classificadas.append({**item, **resultado})
            tokens_total += resultado["tokens_usados"]

        except Exception as e:
            erros += 1
            logger.error("[%s] Erro ao classificar '%s': %s", i + 1, objeto[:80], e)
            classificadas.append({
                **item,
                "categoria": "Outros",
                "confianca": "BAIXA",
                "objeto_vago": False,
                "justificativa_vago": "",
                "resumo": f"Erro na classificação: {str(e)[:120]}",
                "tokens_usados": 0,
                "modelo_usado": OPENAI_MODEL,
                "provider_llm": LLM_PROVIDER,
            })

        if (i + 1) % 10 == 0:
            vagos_ate_agora = sum(1 for c in classificadas if c.get("objeto_vago"))
            logger.info(
                "Progresso: %s/%s | Tokens=%s | Vagos=%s",
                i + 1,
                len(anomalias),
                tokens_total,
                vagos_ate_agora,
            )

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(1)

    caminho_classificadas = salvar_json_em_arquivo(classificadas, "anomalias_classificadas_tmp")
    ti.xcom_push(key="classificacoes_arquivo", value=caminho_classificadas)

    vagos = sum(1 for c in classificadas if c.get("objeto_vago"))
    logger.info(
        "Classificação concluída: %s | Vagos=%s | Erros=%s | Tokens=%s",
        len(classificadas),
        vagos,
        erros,
        tokens_total,
    )

    return len(classificadas)


# =============================================================================
# TASK 6 - SALVAR CLASSIFICAÇÕES LLM
# =============================================================================

def salvar_classificacoes_llm(**context):
    ti = context["ti"]
    caminho_json = ti.xcom_pull(
        task_ids="classificar_anomalias_llm",
        key="classificacoes_arquivo",
    )

    if not caminho_json:
        logger.info("Nenhuma classificação para salvar.")
        return 0

    classificacoes = ler_json_de_arquivo(caminho_json)
    if not classificacoes:
        logger.info("Arquivo de classificações vazio.")
        return 0

    sql_truncate = "TRUNCATE TABLE anomalias_classificadas RESTART IDENTITY;"
    sql_insert = """
        INSERT INTO anomalias_classificadas (
            isn_sic, objeto, fornecedor_nome, orgao_nome, valor_global,
            prazo_vigencia_dias, score_anomalia, percentil_risco, nivel_risco,
            data_assinatura, categoria, confianca, objeto_vago,
            justificativa_vago, resumo, tokens_usados, modelo_usado, provider_llm
        ) VALUES %s
    """

    registros = [
        (
            c.get("isn_sic"),
            c.get("objeto"),
            c.get("fornecedor_nome"),
            c.get("orgao_nome"),
            c.get("valor_global"),
            c.get("prazo_vigencia_dias"),
            float(c.get("score_anomalia", 0)),
            int(c.get("percentil_risco", 0)),
            c.get("nivel_risco"),
            c.get("data_assinatura"),
            c.get("categoria"),
            c.get("confianca"),
            bool(c.get("objeto_vago", False)),
            c.get("justificativa_vago"),
            c.get("resumo"),
            int(c.get("tokens_usados", 0)),
            c.get("modelo_usado"),
            c.get("provider_llm"),
        )
        for c in classificacoes
    ]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_truncate)
            execute_values(cur, sql_insert, registros)
        conn.commit()

    vagos = sum(1 for c in classificacoes if c.get("objeto_vago"))
    logger.info("%s classificações salvas | %s objetos vagos.", len(registros), vagos)

    return len(registros)


# =============================================================================
# TASK 7 - RELATÓRIO CONSOLIDADO
# =============================================================================

def gerar_relatorio_consolidado(**context):
    ti = context["ti"]
    caminho_json = ti.xcom_pull(
        task_ids="classificar_anomalias_llm",
        key="classificacoes_arquivo",
    )

    if not caminho_json:
        logger.info("Sem arquivo de classificações para gerar relatório.")
        return None

    classificacoes = ler_json_de_arquivo(caminho_json)
    if not classificacoes:
        logger.info("Sem dados para relatório.")
        return None

    df = pd.DataFrame(classificacoes)

    if df.empty:
        logger.info("DataFrame vazio. Relatório não gerado.")
        return None

    df["valor_global"] = pd.to_numeric(df["valor_global"], errors="coerce").fillna(0)
    df["percentil_risco"] = pd.to_numeric(df["percentil_risco"], errors="coerce").fillna(0).astype(int)
    df["objeto_vago"] = df["objeto_vago"].fillna(False)

    total = len(df)
    total_valor = df["valor_global"].sum()
    total_vagos = int(df["objeto_vago"].sum())

    por_categoria = (
        df.groupby("categoria", dropna=False)
        .size()
        .reset_index(name="qtd")
        .sort_values("qtd", ascending=False)
    )

    por_orgao = (
        df.groupby("orgao_nome", dropna=False)
        .agg(
            qtd_anomalias=("isn_sic", "count"),
            valor_total=("valor_global", "sum"),
        )
        .reset_index()
        .sort_values(["qtd_anomalias", "valor_total"], ascending=[False, False])
        .head(15)
    )

    por_risco = (
        df.groupby("nivel_risco", dropna=False)
        .agg(
            qtd=("isn_sic", "count"),
            valor_total=("valor_global", "sum"),
        )
        .reset_index()
    )

    vagos = (
        df[df["objeto_vago"] == True][[
            "orgao_nome",
            "objeto",
            "categoria",
            "justificativa_vago",
            "nivel_risco",
        ]]
        .fillna("")
        .head(50)
    )

    # Bloco HTML - distribuição por categoria
    blocos_categoria = ""
    max_qtd = int(por_categoria["qtd"].max()) if not por_categoria.empty else 1

    for _, row in por_categoria.iterrows():
        categoria = row["categoria"] or "Sem categoria"
        qtd = int(row["qtd"])
        pct = round((qtd / total) * 100, 1) if total else 0
        largura = round((qtd / max_qtd) * 100) if max_qtd else 0

        blocos_categoria += f"""
        <div class="bar-row">
          <span class="bar-label">{categoria}</span>
          <div class="bar-wrap"><div class="bar-fill" style="width:{largura}%"></div></div>
          <span class="bar-count">{qtd} ({pct}%)</span>
        </div>
        """

    # Tabela órgãos
    linhas_orgaos = ""
    for _, row in por_orgao.iterrows():
        linhas_orgaos += f"""
        <tr>
          <td>{row['orgao_nome'] or ''}</td>
          <td>{int(row['qtd_anomalias'])}</td>
          <td>{formatar_moeda_br(row['valor_total'])}</td>
        </tr>
        """

    # Tabela por risco
    linhas_risco = ""
    for _, row in por_risco.iterrows():
        linhas_risco += f"""
        <tr>
          <td>{row['nivel_risco'] or ''}</td>
          <td>{int(row['qtd'])}</td>
          <td>{formatar_moeda_br(row['valor_total'])}</td>
        </tr>
        """

    # Tabela vagos
    linhas_vagos = ""
    for _, row in vagos.iterrows():
        objeto = row["objeto"] or ""
        linhas_vagos += f"""
        <tr>
          <td>{row['orgao_nome']}</td>
          <td>{objeto[:140]}{"..." if len(objeto) > 140 else ""}</td>
          <td>{row['categoria']}</td>
          <td>{row['nivel_risco']}</td>
          <td>{(row['justificativa_vago'] or '')[:140]}</td>
        </tr>
        """

    data_exec = datetime.now().strftime("%d/%m/%Y %H:%M")
    ds_nodash = context["ds_nodash"]
    caminho_relatorio = f"/tmp/relatorio_contratos_{ds_nodash}.html"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Relatório Consolidado de Contratos Anômalos</title>
<style>
  body {{ font-family: Arial, sans-serif; margin:0; padding:20px; background:#f5f7fa; color:#333; }}
  .header {{ background:#0D2B55; color:white; padding:24px 32px; border-radius:8px; margin-bottom:24px; }}
  .header h1 {{ margin:0; font-size:24px; }}
  .header p {{ margin:6px 0 0; opacity:.85; font-size:13px; }}
  .cards {{ display:flex; gap:16px; margin-bottom:24px; flex-wrap:wrap; }}
  .card {{ background:white; border-radius:8px; padding:20px 24px; flex:1; min-width:180px; box-shadow:0 1px 4px rgba(0,0,0,.08); }}
  .card .num {{ font-size:30px; font-weight:700; color:#0D2B55; }}
  .card .lbl {{ font-size:12px; color:#666; margin-top:4px; }}
  .section {{ background:white; border-radius:8px; padding:24px; margin-bottom:20px; box-shadow:0 1px 4px rgba(0,0,0,.08); }}
  .section h2 {{ margin-top:0; font-size:16px; color:#0D2B55; border-bottom:2px solid #E3F2FD; padding-bottom:10px; }}
  .bar-row {{ display:flex; align-items:center; gap:12px; margin-bottom:8px; }}
  .bar-label {{ width:220px; font-size:13px; text-align:right; color:#555; }}
  .bar-wrap {{ flex:1; background:#E8ECEF; border-radius:4px; height:18px; }}
  .bar-fill {{ background:#1565C0; border-radius:4px; height:100%; }}
  .bar-count {{ width:90px; font-size:12px; color:#888; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ background:#0D2B55; color:white; padding:10px 12px; text-align:left; }}
  td {{ padding:9px 12px; border-bottom:1px solid #EEE; vertical-align:top; }}
  tr:hover td {{ background:#F5F9FF; }}
  .footer {{ text-align:center; font-size:11px; color:#999; margin-top:24px; }}
</style>
</head>
<body>

<div class="header">
  <h1>Relatório Consolidado de Contratos Anômalos</h1>
  <p>Gerado em {data_exec} | Modelo LLM: {OPENAI_MODEL} | Provider: {LLM_PROVIDER}</p>
</div>

<div class="cards">
  <div class="card">
    <div class="num">{total}</div>
    <div class="lbl">Contratos anômalos classificados</div>
  </div>
  <div class="card">
    <div class="num">{formatar_moeda_br(total_valor)}</div>
    <div class="lbl">Valor total dos contratos anômalos</div>
  </div>
  <div class="card">
    <div class="num">{total_vagos}</div>
    <div class="lbl">Objetos vagos detectados</div>
  </div>
  <div class="card">
    <div class="num">{len(por_categoria)}</div>
    <div class="lbl">Categorias identificadas</div>
  </div>
</div>

<div class="section">
  <h2>Distribuição de contratos anômalos por categoria</h2>
  {blocos_categoria}
</div>

<div class="section">
  <h2>Órgãos com mais anomalias</h2>
  <table>
    <thead>
      <tr>
        <th>Órgão</th>
        <th>Qtd. anomalias</th>
        <th>Valor total suspeito</th>
      </tr>
    </thead>
    <tbody>
      {linhas_orgaos}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Total em R$ por nível de risco</h2>
  <table>
    <thead>
      <tr>
        <th>Nível de risco</th>
        <th>Quantidade</th>
        <th>Valor total</th>
      </tr>
    </thead>
    <tbody>
      {linhas_risco}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Lista de objetos vagos detectados</h2>
  <table>
    <thead>
      <tr>
        <th>Órgão</th>
        <th>Objeto original</th>
        <th>Categoria</th>
        <th>Risco</th>
        <th>Justificativa</th>
      </tr>
    </thead>
    <tbody>
      {linhas_vagos}
    </tbody>
  </table>
</div>

<div class="footer">
  Pipeline ETL + ML + LLM | Relatório salvo em {caminho_relatorio}
</div>

</body>
</html>
"""

    Path(caminho_relatorio).write_text(html, encoding="utf-8")
    ti.xcom_push(key="relatorio_html_path", value=caminho_relatorio)

    logger.info("Relatório consolidado salvo em %s", caminho_relatorio)
    return caminho_relatorio


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="anomalias_v4_llm",
    description=(
        "Extrai contratos do Ceará Transparente, salva no PostgreSQL, "
        "detecta anomalias, classifica com LLM e gera relatório HTML consolidado."
    ),
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["contratos", "anomalias", "ceara-transparente", "ml", "llm", "html"],
    default_args={
        "owner": "carlos_arthur",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:

    task_criar_tabelas = PythonOperator(
        task_id="criar_tabelas",
        python_callable=criar_tabelas,
    )

    task_extrair = PythonOperator(
        task_id="extrair_contratos",
        python_callable=extrair_contratos,
    )

    task_salvar = PythonOperator(
        task_id="salvar_postgres",
        python_callable=salvar_postgres,
    )

    task_detectar_anomalias = PythonOperator(
        task_id="detectar_anomalias",
        python_callable=detectar_anomalias,
    )

    task_salvar_anomalias = PythonOperator(
        task_id="salvar_anomalias",
        python_callable=salvar_anomalias,
    )

    task_classificar_llm = PythonOperator(
        task_id="classificar_anomalias_llm",
        python_callable=classificar_anomalias_llm,
    )

    task_salvar_classificacoes = PythonOperator(
        task_id="salvar_classificacoes_llm",
        python_callable=salvar_classificacoes_llm,
    )

    task_gerar_relatorio = PythonOperator(
        task_id="gerar_relatorio_consolidado",
        python_callable=gerar_relatorio_consolidado,
    )

    task_enviar_email_html = EmailOperator(
        task_id="enviar_email_html",
        to="remetente@email.com",
        subject="Relatório Consolidado de Contratos Anômalos - {{ ds }}",
        html_content="""
        <h3>Relatório consolidado gerado com sucesso</h3>
        <p>Segue em anexo o relatório HTML dos contratos anômalos classificados.</p>
        <p><b>Data da execução:</b> {{ ds }}</p>
        """,
        files=["{{ ti.xcom_pull(task_ids='gerar_relatorio_consolidado', key='relatorio_html_path') }}"],
    )

    (
        task_criar_tabelas
        >> task_extrair
        >> task_salvar
        >> task_detectar_anomalias
        >> task_salvar_anomalias
        >> task_classificar_llm
        >> task_salvar_classificacoes
        >> task_gerar_relatorio
        >> task_enviar_email_html
    )