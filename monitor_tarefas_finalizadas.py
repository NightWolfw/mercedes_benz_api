import psycopg2
import time
import json
import os
from datetime import datetime, timedelta


class MonitorTarefasFinalizadas:
    def __init__(self):
        # Conexão com seu PostgreSQL da Mercedes
        self.db_config = {
            'host': '10.84.224.17',
            'database': 'dw_gps',
            'user': 'gpssa_pg_jonatan_lopes',
            'password': 'rrxD&!C2qU1V',
            'port': 5432
        }

        self.tarefas_em_observacao = self.carregar_tarefas_em_observacao()
        self.tarefas_enviadas = self.carregar_tarefas_enviadas()

        # NOVOS CONTROLES COM LOGS
        self.conexoes_falharam_consecutivas = 0
        self.ultima_consulta_sucesso = None
        self.stats_sessao = {
            'consultas_realizadas': 0,
            'conexoes_falharam': 0,
            'tarefas_finalizadas': 0,
            'tarefas_adicionadas': 0,
            'tempo_total_consultas': 0,
            'inicio_sessao': datetime.now()
        }

        # WhatsApp será injetado pelo main
        self.whatsapp_sender = None

        self.log_detalhado(f"🚀 Monitor Tarefas iniciado")
        self.log_detalhado(f"👀 Tarefas sendo observadas: {len(self.tarefas_em_observacao)}")
        self.log_detalhado(f"✅ Tarefas já finalizadas: {len(self.tarefas_enviadas)}")

    def log_detalhado(self, mensagem, nivel='INFO'):
        """Sistema de log ninja para tarefas"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        emojis = {
            'INFO': '💡',
            'SUCCESS': '✅',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'DEBUG': '🔍',
            'STATS': '📊'
        }

        emoji = emojis.get(nivel, '📝')
        log_line = f"[{timestamp}] {emoji} {nivel} [TAREFAS]: {mensagem}"

        print(log_line)

        # Salva em arquivo separado para tarefas
        try:
            with open('mercedes_tarefas_logs.txt', 'a', encoding='utf-8') as f:
                f.write(log_line + '\n')
        except:
            pass

    def mostrar_stats_sessao(self):
        """Mostra estatísticas da sessão de tarefas"""
        agora = datetime.now()
        tempo_rodando = agora - self.stats_sessao['inicio_sessao']

        tempo_medio_consulta = 0
        if self.stats_sessao['consultas_realizadas'] > 0:
            tempo_medio_consulta = self.stats_sessao['tempo_total_consultas'] / self.stats_sessao[
                'consultas_realizadas']

        self.log_detalhado("=" * 60, 'STATS')
        self.log_detalhado(f"📊 ESTATÍSTICAS MONITOR TAREFAS", 'STATS')
        self.log_detalhado(f"⏱️  Tempo rodando: {str(tempo_rodando).split('.')[0]}", 'STATS')
        self.log_detalhado(f"🔍 Consultas realizadas: {self.stats_sessao['consultas_realizadas']}", 'STATS')
        self.log_detalhado(f"❌ Falhas de conexão: {self.stats_sessao['conexoes_falharam']}", 'STATS')
        self.log_detalhado(f"➕ Tarefas adicionadas: {self.stats_sessao['tarefas_adicionadas']}", 'STATS')
        self.log_detalhado(f"✅ Tarefas finalizadas: {self.stats_sessao['tarefas_finalizadas']}", 'STATS')
        self.log_detalhado(f"⚡ Tempo médio consulta: {tempo_medio_consulta:.2f}s", 'STATS')
        self.log_detalhado(f"👀 Em observação agora: {len(self.tarefas_em_observacao)}", 'STATS')
        if self.ultima_consulta_sucesso:
            self.log_detalhado(f"✅ Última consulta OK: {self.ultima_consulta_sucesso}", 'STATS')
        self.log_detalhado("=" * 60, 'STATS')

    def conectar_bd(self, tentativas_maximas=3):
        """Conecta no PostgreSQL - VERSÃO COM LOGS DETALHADOS"""
        inicio_tentativas = time.time()

        for tentativa in range(tentativas_maximas):
            inicio_tentativa = time.time()

            try:
                self.log_detalhado(f"🔄 Tentativa conexão #{tentativa + 1}/{tentativas_maximas}...")

                conn = psycopg2.connect(
                    host=self.db_config['host'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    port=self.db_config['port'],
                    connect_timeout=20,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=5,
                    keepalives_count=5,
                    sslmode='prefer'
                )

                tempo_conexao = time.time() - inicio_tentativa
                self.log_detalhado(f"✅ Conexão OK em {tempo_conexao:.2f}s", 'SUCCESS')
                self.conexoes_falharam_consecutivas = 0
                return conn

            except Exception as e:
                tempo_tentativa = time.time() - inicio_tentativa
                self.log_detalhado(f"❌ Tentativa #{tentativa + 1} falhou em {tempo_tentativa:.2f}s: {str(e)[:100]}...",
                                   'ERROR')
                self.stats_sessao['conexoes_falharam'] += 1

                if tentativa < tentativas_maximas - 1:
                    tempo_espera = (tentativa + 1) * 5
                    self.log_detalhado(f"⏳ Aguardando {tempo_espera}s...", 'WARNING')
                    time.sleep(tempo_espera)

        tempo_total = time.time() - inicio_tentativas
        self.conexoes_falharam_consecutivas += 1
        self.log_detalhado(f"🚨 Todas tentativas falharam em {tempo_total:.2f}s", 'ERROR')
        return None

    def carregar_tarefas_em_observacao(self):
        """Carrega a lista de tarefas observadas COM LOG"""
        try:
            if os.path.exists('tarefas_observacao.json'):
                with open('tarefas_observacao.json', 'r') as f:
                    tarefas = json.load(f)
                    return tarefas
            return []
        except Exception as e:
            print(f"⚠️ Erro ao carregar observação: {e}")
            return []

    def salvar_tarefas_em_observacao(self):
        """Salva a lista de tarefas em observação COM LOG"""
        try:
            with open('tarefas_observacao.json', 'w', encoding='utf-8') as f:
                json.dump(self.tarefas_em_observacao, f, indent=2, ensure_ascii=False)
            self.log_detalhado(f"💾 Salvas {len(self.tarefas_em_observacao)} tarefas em observação", 'DEBUG')
        except Exception as e:
            self.log_detalhado(f"❌ Erro ao salvar observação: {e}", 'ERROR')

    def carregar_tarefas_enviadas(self):
        """Carrega a lista de tarefas enviadas COM LOG"""
        try:
            if os.path.exists('tarefas_finalizadas_enviadas.json'):
                with open('tarefas_finalizadas_enviadas.json', 'r') as f:
                    tarefas = set(json.load(f))
                    return tarefas
            return set()
        except Exception as e:
            print(f"⚠️ Erro ao carregar enviadas: {e}")
            return set()

    def salvar_tarefas_enviadas(self):
        """Salva a lista de tarefas enviadas COM LOG"""
        try:
            with open('tarefas_finalizadas_enviadas.json', 'w') as f:
                json.dump(list(self.tarefas_enviadas), f)
            self.log_detalhado(f"💾 Salvas {len(self.tarefas_enviadas)} tarefas enviadas", 'DEBUG')
        except Exception as e:
            self.log_detalhado(f"❌ Erro ao salvar enviadas: {e}", 'ERROR')

    def buscar_novas_tarefas_pendentes(self):
        """Pega tarefas novas COM LOGS DETALHADOS"""
        inicio_consulta = time.time()
        data_referencia = (datetime.now() - timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')

        self.log_detalhado(f"🔍 Buscando tarefas pendentes desde: {data_referencia}", 'DEBUG')

        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        query = """
                SELECT T.id             as tarefa_id,
                       T.numero         as numero_tarefa,
                       T.objetoorigemid as chamado_id,
                       T.status         as status_atual,
                       T.criado         as data_criacao
                FROM dbo.tarefa T
                WHERE T.origem = 48
                  AND T.status IN (10, 25)
                  AND T.estruturanivel2 IN ('44462 - SP - MAI - MERCEDES - SBC - MANUT')
                  AND T.criado > %s::timestamp
                ORDER BY T.criado DESC
                    LIMIT 50
                """

        try:
            inicio_query = time.time()
            cursor.execute(query, (data_referencia,))
            tarefas_raw = cursor.fetchall()
            tempo_query = time.time() - inicio_query

            cursor.close()
            conn.close()

            self.log_detalhado(f"🎯 Query executada em {tempo_query:.2f}s - {len(tarefas_raw)} tarefas pendentes",
                               'DEBUG')

            if not tarefas_raw:
                tempo_total = time.time() - inicio_consulta
                self.log_detalhado(f"📭 Nenhuma tarefa pendente nova (consulta: {tempo_total:.2f}s)", 'INFO')
                self.stats_sessao['consultas_realizadas'] += 1
                self.stats_sessao['tempo_total_consultas'] += tempo_total
                self.ultima_consulta_sucesso = datetime.now().strftime('%H:%M:%S')
                return []

            # Log da tarefa mais recente
            tarefa_mais_recente = tarefas_raw[0]
            self.log_detalhado(
                f"🆕 Tarefa pendente mais recente: #{tarefa_mais_recente[1]} (ID: {tarefa_mais_recente[0]}) criada em {tarefa_mais_recente[4]}",
                'INFO')

            # Filtra as já observadas
            ids_ja_observados = {t['id'] for t in self.tarefas_em_observacao}
            tarefas_novas = []

            for t in tarefas_raw:
                if t[0] not in ids_ja_observados:  # t[0] = tarefa_id
                    tarefa_dict = {
                        'tarefa_id': t[0],
                        'numero_tarefa': t[1],
                        'chamado_id': t[2],
                        'status_atual': t[3],
                        'data_criacao': t[4]
                    }
                    tarefas_novas.append(tarefa_dict)

            self.log_detalhado(f"➕ {len(tarefas_novas)} tarefas realmente novas para observar", 'INFO')

            # Busca dados dos chamados se tem tarefas novas
            if tarefas_novas:
                ids_chamados = [t['chamado_id'] for t in tarefas_novas]
                dados_chamados = self.buscar_dados_chamados_por_id(ids_chamados)
                resultado = self.mesclar_tarefas_com_chamados(tarefas_novas, dados_chamados)

                tempo_total = time.time() - inicio_consulta
                self.stats_sessao['consultas_realizadas'] += 1
                self.stats_sessao['tempo_total_consultas'] += tempo_total
                self.ultima_consulta_sucesso = datetime.now().strftime('%H:%M:%S')

                return resultado

            tempo_total = time.time() - inicio_consulta
            self.stats_sessao['consultas_realizadas'] += 1
            self.stats_sessao['tempo_total_consultas'] += tempo_total
            self.ultima_consulta_sucesso = datetime.now().strftime('%H:%M:%S')
            return []

        except Exception as e:
            tempo_total = time.time() - inicio_consulta
            self.log_detalhado(f"💥 Erro na busca após {tempo_total:.2f}s: {e}", 'ERROR')
            return []

    def buscar_dados_chamados_por_id(self, ids_chamados):
        """Query separada pros dados dos chamados COM LOG"""
        if not ids_chamados:
            return {}

        inicio_busca = time.time()
        conn = self.conectar_bd()
        if not conn:
            return {}

        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(ids_chamados))
        query = f"""
        SELECT 
            C.id,
            C.numero,
            C.nome,
            C.emergencial
        FROM dbo.chamado C
        WHERE C.id IN ({placeholders})
        """

        try:
            cursor.execute(query, tuple(ids_chamados))
            chamados_raw = cursor.fetchall()
            cursor.close()
            conn.close()

            tempo_busca = time.time() - inicio_busca
            self.log_detalhado(f"📊 Dados chamados obtidos em {tempo_busca:.2f}s", 'DEBUG')

            return {c[0]: {'numero_chamado': c[1], 'local': c[2], 'emergencial': c[3]} for c in chamados_raw}

        except Exception as e:
            tempo_busca = time.time() - inicio_busca
            self.log_detalhado(f"💥 Erro buscar chamados após {tempo_busca:.2f}s: {e}", 'ERROR')
            return {}

    def mesclar_tarefas_com_chamados(self, tarefas, dados_chamados):
        """Junta dados com log"""
        resultado = []

        for tarefa in tarefas:
            chamado_data = dados_chamados.get(tarefa['chamado_id'], {})

            tarefa_completa = {
                'tarefa_id': tarefa['tarefa_id'],
                'numero_tarefa': tarefa['numero_tarefa'],
                'status_atual': tarefa['status_atual'],
                'data_criacao': tarefa['data_criacao'],
                'numero_chamado': chamado_data.get('numero_chamado', 'N/A'),
                'local': chamado_data.get('local', 'N/A'),
                'emergencial': chamado_data.get('emergencial', False)
            }

            resultado.append(tarefa_completa)
            self.log_detalhado(
                f"🔗 Mesclada tarefa {tarefa['numero_tarefa']} com chamado {chamado_data.get('numero_chamado', 'N/A')}",
                'DEBUG')

        return resultado

    def verificar_status_tarefas_observadas(self):
        """Verifica se tarefas mudaram pra finalizadas COM LOGS DETALHADOS"""
        if not self.tarefas_em_observacao:
            self.log_detalhado("👀 Nenhuma tarefa em observação para verificar", 'INFO')
            return []

        inicio_verificacao = time.time()
        self.log_detalhado(f"🔍 Verificando status de {len(self.tarefas_em_observacao)} tarefas...", 'INFO')

        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        ids_observados = [t['id'] for t in self.tarefas_em_observacao]
        placeholders = ','.join(['%s'] * len(ids_observados))

        query = f"""
        SELECT 
            T.id as tarefa_id,
            T.numero as numero_tarefa,
            T.status as status_atual,
            T.terminoreal as data_finalizacao
        FROM dbo.tarefa T
        WHERE T.id IN ({placeholders})
        """

        try:
            cursor.execute(query, tuple(ids_observados))
            tarefas_raw = cursor.fetchall()
            cursor.close()
            conn.close()

            # Separa finalizadas das pendentes
            ids_finalizadas = []
            tarefas_ainda_pendentes = []
            finalizadas_encontradas = 0

            for t in tarefas_raw:
                if t[2] == 85:  # status_atual == 85 (finalizada)
                    ids_finalizadas.append(t[0])  # tarefa_id
                    finalizadas_encontradas += 1
                    self.log_detalhado(f"🎉 Tarefa {t[1]} FINALIZADA!", 'SUCCESS')
                else:
                    # Mantém na observação
                    tarefa_obs = {
                        'id': str(t[0]),
                        'numero_tarefa': t[1],
                        'status': t[2]
                    }
                    tarefas_ainda_pendentes.append(tarefa_obs)

            # Log do resultado da verificação
            tempo_verificacao = time.time() - inicio_verificacao
            self.log_detalhado(f"📊 Verificação concluída em {tempo_verificacao:.2f}s:", 'INFO')
            self.log_detalhado(f"   ✅ Finalizadas: {finalizadas_encontradas}", 'INFO')
            self.log_detalhado(f"   ⏳ Ainda pendentes: {len(tarefas_ainda_pendentes)}", 'INFO')

            # Atualiza lista de observação
            self.tarefas_em_observacao = tarefas_ainda_pendentes
            self.salvar_tarefas_em_observacao()

            # Busca dados completos das finalizadas
            if ids_finalizadas:
                self.log_detalhado(f"🔍 Buscando dados completos de {len(ids_finalizadas)} tarefa(s) finalizada(s)...",
                                   'INFO')
                return self.buscar_dados_tarefa_finalizada_lista(ids_finalizadas)

            return []

        except Exception as e:
            tempo_verificacao = time.time() - inicio_verificacao
            self.log_detalhado(f"💥 Erro verificação após {tempo_verificacao:.2f}s: {e}", 'ERROR')
            return []

    def buscar_dados_tarefa_finalizada_lista(self, ids_tarefas):
        """Busca dados completos das finalizadas COM LOGS E REALIZADOR"""
        inicio_busca = time.time()

        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(ids_tarefas))

        # Query principal das tarefas
        query = f"""
        SELECT 
            T.id as tarefa_id,
            T.numero as numero_tarefa,
            T.terminoreal as data_finalizacao,
            C.numero as numero_chamado,
            C.nome as local,
            C.emergencial,
            R.nome as realizador_nome
        FROM dbo.tarefa T
        LEFT JOIN dbo.chamado C ON C.id = T.objetoorigemid
        LEFT JOIN dbo.recurso R ON R.codigohash = T.finalizadoporhash
        WHERE T.id IN ({placeholders})
        """

        try:
            cursor.execute(query, tuple(ids_tarefas))
            tarefas_raw = cursor.fetchall()

            # Busca execuções
            self.log_detalhado(f"🔍 Buscando execuções das tarefas finalizadas...", 'DEBUG')
            execucoes_por_tarefa = self.buscar_execucoes_das_tarefas(cursor, ids_tarefas)

            cursor.close()
            conn.close()

            tempo_busca = time.time() - inicio_busca
            self.log_detalhado(f"📊 Dados completos obtidos em {tempo_busca:.2f}s", 'SUCCESS')

            resultado = []
            for t in tarefas_raw:
                tarefa_dict = {
                    'numero_tarefa': t[1],
                    'data_finalizacao': t[2],
                    'numero_chamado': t[3],
                    'local': t[4],
                    'emergencial': t[5],
                    'realizador_nome': t[6] or 'Não informado'
                }

                # Adiciona info das execuções
                tarefa_id = t[0]
                if tarefa_id in execucoes_por_tarefa:
                    execucoes = execucoes_por_tarefa[tarefa_id]
                    tarefa_dict['status_maquina'] = execucoes.get('status_maquina', '')
                    tarefa_dict['descricao_atividade'] = execucoes.get('descricao_atividade', '')
                    self.log_detalhado(
                        f"   📋 Tarefa {t[1]}: realizador={t[6] or 'N/A'}, status_maq={execucoes.get('status_maquina', '')[:30]}...",
                        'DEBUG')
                else:
                    tarefa_dict['status_maquina'] = ''
                    tarefa_dict['descricao_atividade'] = ''
                    self.log_detalhado(f"   📋 Tarefa {t[1]}: realizador={t[6] or 'N/A'}, sem execuções", 'DEBUG')

                resultado.append(tarefa_dict)

            return resultado

        except Exception as e:
            tempo_busca = time.time() - inicio_busca
            self.log_detalhado(f"💥 Erro buscar finalizadas após {tempo_busca:.2f}s: {e}", 'ERROR')
            return []

    def buscar_execucoes_das_tarefas(self, cursor, ids_tarefas):
        """Busca execuções COM LOG"""
        if not ids_tarefas:
            return {}

        placeholders = ','.join(['%s'] * len(ids_tarefas))

        query_execucoes = f"""
        SELECT 
            tarefaid,
            perguntaid,
            conteudo
        FROM dbo.execucao 
        WHERE tarefaid IN ({placeholders})
          AND perguntaid IN ('d00fa280-5460-4704-9278-e70e8761f700', 'ec284022-cb0d-4cdd-a681-c3f313010504')
        """

        try:
            cursor.execute(query_execucoes, tuple(ids_tarefas))
            execucoes_raw = cursor.fetchall()

            self.log_detalhado(f"📝 Encontradas {len(execucoes_raw)} execuções relevantes", 'DEBUG')

            # Organiza por tarefa
            execucoes_organizadas = {}

            for exec_row in execucoes_raw:
                tarefa_id = exec_row[0]
                pergunta_id = exec_row[1]
                conteudo = exec_row[2] or ''

                if tarefa_id not in execucoes_organizadas:
                    execucoes_organizadas[tarefa_id] = {}

                # Identifica tipo de info
                if pergunta_id == 'd00fa280-5460-4704-9278-e70e8761f700':
                    execucoes_organizadas[tarefa_id]['status_maquina'] = conteudo.strip()
                elif pergunta_id == 'ec284022-cb0d-4cdd-a681-c3f313010504':
                    execucoes_organizadas[tarefa_id]['descricao_atividade'] = conteudo.strip()

            return execucoes_organizadas

        except Exception as e:
            self.log_detalhado(f"💥 Erro buscar execuções: {e}", 'ERROR')
            return {}

    def adicionar_novas_tarefas_observacao(self):
        """Adiciona novas tarefas na observação COM LOGS DETALHADOS"""
        inicio_adicao = time.time()

        novas_tarefas = self.buscar_novas_tarefas_pendentes()

        if novas_tarefas:
            self.log_detalhado(f"➕ Adicionando {len(novas_tarefas)} nova(s) tarefa(s) na observação", 'SUCCESS')

            for i, tarefa in enumerate(novas_tarefas, 1):
                tarefa_observacao = {
                    'id': tarefa['tarefa_id'],
                    'numero_tarefa': tarefa['numero_tarefa'],
                    'numero_chamado': tarefa['numero_chamado'],
                    'status': tarefa['status_atual'],
                    'emergencial': tarefa['emergencial'],
                    'data_adicao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                self.tarefas_em_observacao.append(tarefa_observacao)
                self.log_detalhado(
                    f"   {i}. Tarefa {tarefa['numero_tarefa']} do chamado {tarefa['numero_chamado']} {'🚨' if tarefa['emergencial'] else ''}",
                    'INFO')

            self.salvar_tarefas_em_observacao()
            self.stats_sessao['tarefas_adicionadas'] += len(novas_tarefas)

            tempo_adicao = time.time() - inicio_adicao
            self.log_detalhado(f"✅ {len(novas_tarefas)} tarefa(s) adicionada(s) em {tempo_adicao:.2f}s", 'SUCCESS')
        else:
            self.log_detalhado("📭 Nenhuma tarefa nova para observar", 'INFO')

    def processar_tarefas_finalizadas(self, tarefas_finalizadas):
        """Processa tarefas finalizadas COM LOGS DETALHADOS"""
        self.log_detalhado(f"🎉 Processando {len(tarefas_finalizadas)} tarefa(s) finalizada(s)!", 'SUCCESS')

        for i, tarefa in enumerate(tarefas_finalizadas, 1):
            numero_tarefa = str(tarefa['numero_tarefa'])

            # Verifica se já foi enviada
            if numero_tarefa in self.tarefas_enviadas:
                self.log_detalhado(f"⚠️ Tarefa {numero_tarefa} já foi enviada antes", 'WARNING')
                continue

            self.log_detalhado("=" * 60, 'SUCCESS')
            self.log_detalhado(f"🎯 PROCESSANDO TAREFA FINALIZADA #{i}", 'SUCCESS')
            self.log_detalhado("=" * 60, 'SUCCESS')
            self.log_detalhado(f"📋 Chamado: {tarefa['numero_chamado']}", 'INFO')
            self.log_detalhado(f"🔧 Tarefa: {tarefa['numero_tarefa']}", 'INFO')
            self.log_detalhado(f"👤 Realizada por: {tarefa['realizador_nome']}", 'INFO')
            self.log_detalhado(f"🕐 Finalizada em: {tarefa['data_finalizacao']}", 'INFO')
            if tarefa.get('status_maquina'):
                self.log_detalhado(f"⚙️ Status máquina: {tarefa['status_maquina'][:50]}...", 'INFO')
            if tarefa.get('descricao_atividade'):
                self.log_detalhado(f"🔧 Atividade: {tarefa['descricao_atividade'][:50]}...", 'INFO')
            self.log_detalhado("=" * 60, 'SUCCESS')

            # Envia pro WhatsApp
            if hasattr(self, 'whatsapp_sender') and self.whatsapp_sender:
                self.log_detalhado("📱 Enviando para WhatsApp...", 'INFO')
                inicio_whatsapp = time.time()

                try:
                    sucesso = self.whatsapp_sender.enviar_tarefa_finalizada(tarefa)
                    tempo_whatsapp = time.time() - inicio_whatsapp

                    if sucesso:
                        self.log_detalhado(f"✅ WhatsApp enviado em {tempo_whatsapp:.2f}s!", 'SUCCESS')
                        self.tarefas_enviadas.add(numero_tarefa)
                        self.salvar_tarefas_enviadas()
                        self.stats_sessao['tarefas_finalizadas'] += 1
                    else:
                        self.log_detalhado(f"⚠️ Falha no WhatsApp após {tempo_whatsapp:.2f}s", 'WARNING')
                except Exception as e:
                    tempo_whatsapp = time.time() - inicio_whatsapp
                    self.log_detalhado(f"❌ Erro WhatsApp após {tempo_whatsapp:.2f}s: {e}", 'ERROR')
            else:
                self.log_detalhado("📱 WhatsApp não configurado", 'WARNING')

            self.log_detalhado("=" * 60, 'SUCCESS')

    def testar_conexao_rapida(self):
        """Teste rápido de conexão"""
        try:
            conn = self.conectar_bd(tentativas_maximas=1)
            if conn:
                conn.close()
                return True
            return False
        except:
            return False

    def rodar_monitor(self):
        """Loop principal NINJA COM CONTROLE INTELIGENTE"""
        self.log_detalhado("🚀 MONITOR INTELIGENTE DE TAREFAS INICIANDO...", 'SUCCESS')
        self.log_detalhado("🧠 Lógica: Observa tarefas pendentes até serem finalizadas", 'INFO')
        self.log_detalhado(f"🏢 Servidor: {self.db_config['host']}", 'INFO')
        self.log_detalhado(f"💽 Database: {self.db_config['database']}", 'INFO')
        self.log_detalhado(f"🛡️ MODO RESILIENTE ATIVO!", 'SUCCESS')
        self.log_detalhado("-" * 50, 'INFO')

        contador_ciclos = 0
        contador_stats = 0

        while True:
            try:
                contador_ciclos += 1
                contador_stats += 1

                self.log_detalhado(f"🔄 Ciclo #{contador_ciclos} iniciado", 'INFO')

                # Mostra stats a cada 15 ciclos (pra não poluir muito)
                if contador_stats >= 15:
                    self.mostrar_stats_sessao()
                    contador_stats = 0

                # Testa conexão antes de processar
                if not self.testar_conexao_rapida():
                    self.log_detalhado("🚨 Banco indisponível!", 'ERROR')
                    self.conexoes_falharam_consecutivas += 1

                    # Intervalo inteligente baseado nas falhas
                    if self.conexoes_falharam_consecutivas <= 3:
                        tempo_espera = 60  # 1 minuto nas primeiras falhas
                        self.log_detalhado(f"⏳ Primeira rodada de falhas - aguardando {tempo_espera}s...", 'WARNING')
                    elif self.conexoes_falharam_consecutivas <= 6:
                        tempo_espera = 120  # 2 minutos se persistir
                        self.log_detalhado(f"⏳ Falhas persistindo - aguardando {tempo_espera}s...", 'WARNING')
                    else:
                        tempo_espera = 300  # 5 minutos se tá muito ruim
                        self.log_detalhado(f"🚨 Sistema muito instável - aguardando {tempo_espera}s...", 'ERROR')

                    time.sleep(tempo_espera)
                    continue

                # Reset contador se conectou
                if self.conexoes_falharam_consecutivas > 0:
                    self.log_detalhado(f"✅ Conexão restaurada após {self.conexoes_falharam_consecutivas} falha(s)!",
                                       'SUCCESS')
                    self.conexoes_falharam_consecutivas = 0

                # ETAPA 1: Adiciona novas tarefas na observação
                try:
                    self.log_detalhado("📝 ETAPA 1: Buscando novas tarefas para observar...", 'INFO')
                    self.adicionar_novas_tarefas_observacao()
                except Exception as e:
                    self.log_detalhado(f"💥 Erro na etapa 1: {e}", 'ERROR')

                # ETAPA 2: Verifica se tarefas observadas foram finalizadas
                try:
                    self.log_detalhado("🔍 ETAPA 2: Verificando tarefas observadas...", 'INFO')
                    tarefas_finalizadas = self.verificar_status_tarefas_observadas()

                    if tarefas_finalizadas:
                        self.log_detalhado(f"🎉 ATENÇÃO: {len(tarefas_finalizadas)} tarefa(s) FINALIZADAS!", 'SUCCESS')
                        self.processar_tarefas_finalizadas(tarefas_finalizadas)
                    else:
                        self.log_detalhado(f"👀 Observando {len(self.tarefas_em_observacao)} tarefa(s) pendente(s)...",
                                           'INFO')

                except Exception as e:
                    self.log_detalhado(f"💥 Erro na etapa 2: {e}", 'ERROR')

                # Intervalo inteligente baseado na performance
                if self.stats_sessao['consultas_realizadas'] > 0:
                    tempo_medio = self.stats_sessao['tempo_total_consultas'] / self.stats_sessao['consultas_realizadas']

                    # Ajusta intervalo conforme performance
                    if tempo_medio > 10:  # Sistema lento
                        intervalo = 180  # 3 minutos
                        self.log_detalhado(f"🐌 Sistema lento (média: {tempo_medio:.1f}s) - intervalo 3min", 'WARNING')
                    elif tempo_medio > 5:  # Sistema médio
                        intervalo = 150  # 2.5 minutos
                        self.log_detalhado(f"⚡ Sistema médio (média: {tempo_medio:.1f}s) - intervalo 2.5min", 'INFO')
                    else:  # Sistema rápido
                        intervalo = 120  # 2 minutos
                        self.log_detalhado(f"🚀 Sistema rápido (média: {tempo_medio:.1f}s) - intervalo 2min", 'SUCCESS')
                else:
                    intervalo = 120  # Default

                self.log_detalhado(f"😴 Dormindo por {intervalo}s...", 'INFO')
                time.sleep(intervalo)

            except KeyboardInterrupt:
                self.log_detalhado("\n👋 Monitor parado pelo usuário (Ctrl+C)", 'INFO')
                self.mostrar_stats_sessao()
                break

            except Exception as e:
                self.log_detalhado(f"💥 Erro inesperado: {e}", 'ERROR')
                self.conexoes_falharam_consecutivas += 1
                tempo_espera = min(60 * self.conexoes_falharam_consecutivas, 300)
                self.log_detalhado(f"⏳ Recuperando em {tempo_espera}s...", 'WARNING')
                time.sleep(tempo_espera)


# Teste direto
if __name__ == "__main__":
    print("🎯 MONITOR INTELIGENTE DE TAREFAS - MERCEDES")
    print("=" * 50)

    monitor = MonitorTarefasFinalizadas()
    monitor.rodar_monitor()