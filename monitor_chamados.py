import psycopg2
import time
import json
import os
from datetime import datetime, timedelta
import threading
import queue


class MonitorChamadosMercedes:
    def __init__(self):
        # Conexão com seu PostgreSQL da Mercedes
        self.db_config = {
            'host': '10.84.224.17',
            'database': 'dw_gps',
            'user': 'gpssa_pg_jonatan_lopes',
            'password': 'rrxD&!C2qU1V',
            'port': 5432
        }
        self.ultima_data_processada = self.get_ultima_data_salva()
        self.chamados_enviados = self.carregar_chamados_enviados()

        # NOVOS CONTROLES DE PERFORMANCE
        self.fila_processamento = queue.Queue()
        self.conexoes_falharam_consecutivas = 0
        self.ultima_consulta_sucesso = None
        self.tempo_ultima_consulta = 0
        self.stats_sessao = {
            'consultas_realizadas': 0,
            'conexoes_falharam': 0,
            'chamados_processados': 0,
            'tempo_total_consultas': 0,
            'inicio_sessao': datetime.now()
        }

        self.log_detalhado(f"🚀 Monitor iniciado - Última data: {self.ultima_data_processada}")
        self.log_detalhado(f"📋 Chamados já enviados: {len(self.chamados_enviados)}")

    def log_detalhado(self, mensagem, nivel='INFO'):
        """Sistema de log ninja com timestamps e níveis"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Emojis por nível pra ficar mais fácil de ler
        emojis = {
            'INFO': '💡',
            'SUCCESS': '✅',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'DEBUG': '🔍',
            'STATS': '📊'
        }

        emoji = emojis.get(nivel, '📝')
        log_line = f"[{timestamp}] {emoji} {nivel}: {mensagem}"

        print(log_line)

        # Salva também em arquivo pra histórico
        try:
            with open('mercedes_monitor_logs.txt', 'a', encoding='utf-8') as f:
                f.write(log_line + '\n')
        except:
            pass  # Não para o sistema se não conseguir salvar log

    def mostrar_stats_sessao(self):
        """Mostra estatísticas da sessão atual"""
        agora = datetime.now()
        tempo_rodando = agora - self.stats_sessao['inicio_sessao']

        tempo_medio_consulta = 0
        if self.stats_sessao['consultas_realizadas'] > 0:
            tempo_medio_consulta = self.stats_sessao['tempo_total_consultas'] / self.stats_sessao[
                'consultas_realizadas']

        self.log_detalhado("=" * 60, 'STATS')
        self.log_detalhado(f"📊 ESTATÍSTICAS DA SESSÃO", 'STATS')
        self.log_detalhado(f"⏱️  Tempo rodando: {str(tempo_rodando).split('.')[0]}", 'STATS')
        self.log_detalhado(f"🔍 Consultas realizadas: {self.stats_sessao['consultas_realizadas']}", 'STATS')
        self.log_detalhado(f"❌ Falhas de conexão: {self.stats_sessao['conexoes_falharam']}", 'STATS')
        self.log_detalhado(f"📨 Chamados processados: {self.stats_sessao['chamados_processados']}", 'STATS')
        self.log_detalhado(f"⚡ Tempo médio por consulta: {tempo_medio_consulta:.2f}s", 'STATS')
        if self.ultima_consulta_sucesso:
            self.log_detalhado(f"✅ Última consulta OK: {self.ultima_consulta_sucesso}", 'STATS')
        self.log_detalhado("=" * 60, 'STATS')

    def detectar_erro_ssl_eof(self, erro_str):
        """Detecta se o erro é relacionado a SSL EOF - MODO BERSERKER ATIVADO"""
        indicadores_ssl_eof = [
            'SSL SYSCALL error: EOF detected',
            'SSL connection has been closed unexpectedly',
            'SSL: UNEXPECTED_EOF_WHILE_READING',
            'connection closed',
            'server closed the connection unexpectedly',
            'EOF detected'
        ]

        erro_lower = str(erro_str).lower()
        for indicador in indicadores_ssl_eof:
            if indicador.lower() in erro_lower:
                return True
        return False

    def modo_berserker_conexao(self, max_tentativas=50):
        """
        MODO BERSERKER! 🔥
        Fica tentando conectar até conseguir quando rola erro de SSL EOF
        """
        self.log_detalhado("🔥🔥🔥 MODO BERSERKER ATIVADO! 🔥🔥🔥", 'WARNING')
        self.log_detalhado("💀 Vai tentar conectar até conseguir ou morrer tentando!", 'WARNING')

        tentativa = 0
        tempo_inicio_berserker = time.time()

        while tentativa < max_tentativas:
            tentativa += 1
            inicio_tentativa = time.time()

            self.log_detalhado(f"⚔️ BERSERKER - Tentativa #{tentativa}/{max_tentativas}", 'WARNING')

            try:
                # Tenta conectar com configurações mais agressivas
                conn = psycopg2.connect(
                    host=self.db_config['host'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    port=self.db_config['port'],
                    connect_timeout=15,  # Timeout menor pra tentar mais rápido
                    keepalives=1,
                    keepalives_idle=10,  # Mais agressivo
                    keepalives_interval=2,  # Verifica conexão mais rápido
                    keepalives_count=3,
                    sslmode='prefer'
                )

                # Testa a conexão fazendo uma query simples
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()

                tempo_tentativa = time.time() - inicio_tentativa
                tempo_total_berserker = time.time() - tempo_inicio_berserker

                self.log_detalhado(f"🎯 BERSERKER SUCESSO! Conectou na tentativa #{tentativa}", 'SUCCESS')
                self.log_detalhado(f"⚡ Tempo desta tentativa: {tempo_tentativa:.2f}s", 'SUCCESS')
                self.log_detalhado(f"🏆 Tempo total modo berserker: {tempo_total_berserker:.2f}s", 'SUCCESS')
                self.log_detalhado("🔥 BERSERKER MODE OFF - Voltando ao normal", 'SUCCESS')

                self.conexoes_falharam_consecutivas = 0
                return conn

            except Exception as e:
                tempo_tentativa = time.time() - inicio_tentativa

                # Se for SSL EOF de novo, continua no berserker
                if self.detectar_erro_ssl_eof(str(e)):
                    self.log_detalhado(f"💀 Tentativa #{tentativa} - SSL EOF de novo em {tempo_tentativa:.2f}s", 'ERROR')

                    # Intervalo progressivo mas não muito longo no modo berserker
                    if tentativa <= 10:
                        intervalo = 2  # 2s nas primeiras 10
                    elif tentativa <= 20:
                        intervalo = 5  # 5s nas próximas 10
                    elif tentativa <= 30:
                        intervalo = 10  # 10s nas próximas 10
                    else:
                        intervalo = 15  # 15s nas últimas

                else:
                    # Outros erros, intervalo maior
                    self.log_detalhado(
                        f"❌ Tentativa #{tentativa} - Outro erro em {tempo_tentativa:.2f}s: {str(e)[:100]}", 'ERROR')
                    intervalo = min(tentativa * 2, 30)  # Máximo 30s

                if tentativa < max_tentativas:
                    self.log_detalhado(f"⏳ Berserker aguardando {intervalo}s antes da próxima investida...", 'WARNING')
                    time.sleep(intervalo)

        # Se chegou até aqui, esgotou todas as tentativas
        tempo_total_berserker = time.time() - tempo_inicio_berserker
        self.log_detalhado(f"💀 BERSERKER FALHOU! {max_tentativas} tentativas em {tempo_total_berserker:.2f}s", 'ERROR')
        self.log_detalhado("😵 Saindo do modo berserker... Sistema muito instável", 'ERROR')

        return None

    def conectar_bd_resiliente(self, tentativas_normais=3):
        """
        Conexão ULTRA RESILIENTE que detecta SSL EOF e ativa modo berserker
        """
        inicio_tentativas = time.time()

        # FASE 1: Tentativas normais
        for tentativa in range(tentativas_normais):
            inicio_tentativa = time.time()

            try:
                self.log_detalhado(f"🔄 Tentativa normal #{tentativa + 1}/{tentativas_normais}...")

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
                self.log_detalhado(f"✅ Conexão normal OK em {tempo_conexao:.2f}s", 'SUCCESS')
                self.conexoes_falharam_consecutivas = 0
                return conn

            except Exception as e:
                tempo_tentativa = time.time() - inicio_tentativa
                erro_str = str(e)

                self.log_detalhado(f"❌ Tentativa #{tentativa + 1} falhou em {tempo_tentativa:.2f}s", 'ERROR')
                self.log_detalhado(f"   Erro: {erro_str[:150]}...", 'ERROR')
                self.stats_sessao['conexoes_falharam'] += 1

                # DETECTA SSL EOF e ativa berserker imediatamente
                if self.detectar_erro_ssl_eof(erro_str):
                    self.log_detalhado("🚨 SSL EOF DETECTADO! Ativando modo berserker AGORA!", 'WARNING')
                    return self.modo_berserker_conexao()

                # Se não é SSL EOF, aguarda um pouco antes da próxima
                if tentativa < tentativas_normais - 1:
                    tempo_espera = (tentativa + 1) * 5
                    self.log_detalhado(f"⏳ Aguardando {tempo_espera}s...", 'WARNING')
                    time.sleep(tempo_espera)

        # FASE 2: Se as tentativas normais falharam mas não foi SSL EOF
        tempo_total = time.time() - inicio_tentativas
        self.conexoes_falharam_consecutivas += 1
        self.log_detalhado(f"🚨 Tentativas normais falharam em {tempo_total:.2f}s", 'ERROR')
        self.log_detalhado("🔥 Ativando modo berserker como último recurso...", 'WARNING')

        return self.modo_berserker_conexao()

    def buscar_novos_chamados_resiliente(self):
        """
        Versão resiliente que usa a nova conexão berserker
        """
        inicio_consulta = time.time()
        self.log_detalhado(f"🔍 Iniciando busca RESILIENTE desde: {self.ultima_data_processada}", 'DEBUG')

        # USA A NOVA CONEXÃO RESILIENTE
        conn = self.conectar_bd_resiliente()
        if not conn:
            self.log_detalhado("💀 Conexão falhou mesmo no modo berserker!", 'ERROR')
            return []

        cursor = conn.cursor()

        query = """
                SELECT T.numero           AS numero_tarefa,
                       T.criado           AS data_criacao,
                       T.solicitantenome  AS solicitante,
                       T.servicodescricao AS tipo_servico,
                       T.descricao        AS descricao_tarefa,
                       T.objetoorigemid   AS chamado_id
                FROM dbo.tarefa T
                WHERE T.estruturanivel2 IN ('44462 - SP - MAI - MERCEDES - SBC - MANUT')
                  AND T.origem = 48
                  AND T.criado > %s::timestamp
                ORDER BY T.criado DESC
                    LIMIT 50
                """

        try:
            inicio_query = time.time()
            cursor.execute(query, (self.ultima_data_processada,))
            tarefas_raw = cursor.fetchall()
            tempo_query = time.time() - inicio_query

            cursor.close()
            conn.close()

            self.log_detalhado(f"🎯 Query resiliente executada em {tempo_query:.2f}s - {len(tarefas_raw)} registros",
                               'DEBUG')

            # Resto do código igual ao método original...
            if not tarefas_raw:
                tempo_total = time.time() - inicio_consulta
                self.log_detalhado(f"📭 Nenhuma tarefa nova (consulta resiliente: {tempo_total:.2f}s)", 'INFO')
                self.stats_sessao['consultas_realizadas'] += 1
                self.stats_sessao['tempo_total_consultas'] += tempo_total
                self.ultima_consulta_sucesso = datetime.now().strftime('%H:%M:%S')
                return []

            # Resto da lógica igual...
            tarefa_mais_recente = tarefas_raw[0]
            self.log_detalhado(f"🆕 Tarefa mais recente: #{tarefa_mais_recente[0]} criada em {tarefa_mais_recente[1]}",
                               'INFO')

            ids_chamados = [t[5] for t in tarefas_raw]
            self.log_detalhado(f"🔄 Buscando dados de {len(ids_chamados)} chamados...", 'DEBUG')

            dados_chamados = self.buscar_dados_chamados_resiliente(ids_chamados)

            resultado = []
            for t in tarefas_raw:
                chamado_data = dados_chamados.get(t[5], {})

                chamado_completo = {
                    'numero_tarefa': t[0],
                    'data_criacao': t[1],
                    'solicitante': t[2],
                    'tipo_servico': t[3],
                    'descricao_tarefa': t[4],
                    'numero_chamado': chamado_data.get('numero_chamado', 'N/A'),
                    'local': chamado_data.get('local', 'N/A'),
                    'descricao_chamado': chamado_data.get('descricao_chamado', 'N/A'),
                    'emergencial': chamado_data.get('emergencial', False),
                    'telefone': chamado_data.get('telefone', 'N/A')
                }
                resultado.append(chamado_completo)

            tempo_total = time.time() - inicio_consulta
            self.stats_sessao['consultas_realizadas'] += 1
            self.stats_sessao['tempo_total_consultas'] += tempo_total
            self.ultima_consulta_sucesso = datetime.now().strftime('%H:%M:%S')

            self.log_detalhado(f"✅ Consulta resiliente completa em {tempo_total:.2f}s - {len(resultado)} chamados",
                               'SUCCESS')
            return resultado

        except Exception as e:
            tempo_total = time.time() - inicio_consulta

            # Se der SSL EOF na query, tenta berserker de novo
            if self.detectar_erro_ssl_eof(str(e)):
                self.log_detalhado(f"🔥 SSL EOF na query! Tentando berserker novamente...", 'WARNING')
                cursor.close()
                conn.close()
                # Tenta uma vez mais com berserker
                return self.buscar_novos_chamados_resiliente()

            self.log_detalhado(f"💥 Erro na consulta resiliente após {tempo_total:.2f}s: {e}", 'ERROR')
            cursor.close()
            conn.close()
            return []

    def buscar_dados_chamados_resiliente(self, ids_chamados):
        """Versão resiliente para buscar dados dos chamados"""
        if not ids_chamados:
            return {}

        inicio_busca = time.time()
        conn = self.conectar_bd_resiliente()
        if not conn:
            return {}

        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(ids_chamados))
        query = f"""
        SELECT 
            C.id,
            C.numero,
            C.nome,
            C.descricao,
            C.emergencial,
            C.solicitantetelefone
        FROM dbo.chamado C
        WHERE C.id IN ({placeholders})
        """

        try:
            cursor.execute(query, tuple(ids_chamados))
            chamados_raw = cursor.fetchall()
            cursor.close()
            conn.close()

            tempo_busca = time.time() - inicio_busca
            self.log_detalhado(f"📊 Dados chamados resilientes em {tempo_busca:.2f}s", 'DEBUG')

            return {
                c[0]: {
                    'numero_chamado': c[1],
                    'local': c[2],
                    'descricao_chamado': c[3],
                    'emergencial': c[4],
                    'telefone': c[5]
                } for c in chamados_raw
            }

        except Exception as e:
            tempo_busca = time.time() - inicio_busca

            if self.detectar_erro_ssl_eof(str(e)):
                self.log_detalhado(f"🔥 SSL EOF nos dados! Tentando berserker...", 'WARNING')
                cursor.close()
                conn.close()
                return self.buscar_dados_chamados_resiliente(ids_chamados)

            self.log_detalhado(f"💥 Erro buscar dados resiliente após {tempo_busca:.2f}s: {e}", 'ERROR')
            cursor.close()
            conn.close()
            return {}

    def processar_novos_chamados_resiliente(self):
        """Versão resiliente que usa conexões berserker"""
        self.log_detalhado("🔄 Iniciando processamento RESILIENTE...", 'INFO')

        # USA A VERSÃO RESILIENTE
        novos_chamados = self.buscar_novos_chamados_resiliente()

        if not novos_chamados:
            return

        # Resto da lógica igual ao método original...
        self.log_detalhado(f"📋 {len(novos_chamados)} chamado(s) encontrado(s)", 'INFO')

        chamados_realmente_novos = []
        chamados_ja_enviados = 0

        for chamado in novos_chamados:
            numero_chamado = str(chamado['numero_chamado'])
            if numero_chamado not in self.chamados_enviados:
                chamados_realmente_novos.append(chamado)
            else:
                chamados_ja_enviados += 1

        if chamados_ja_enviados > 0:
            self.log_detalhado(f"🔄 {chamados_ja_enviados} já processados", 'INFO')

        if not chamados_realmente_novos:
            self.log_detalhado("✅ Todos já foram processados", 'INFO')
            return

        self.log_detalhado(f"🆕 {len(chamados_realmente_novos)} REALMENTE NOVOS!", 'SUCCESS')

        for i, chamado in enumerate(chamados_realmente_novos, 1):
            self.log_detalhado(f"⚡ Processando {i}/{len(chamados_realmente_novos)}: {chamado['numero_chamado']}",
                               'INFO')

            self.processar_chamado_individual(chamado)
            self.marcar_chamado_como_enviado(chamado['numero_chamado'])
            self.stats_sessao['chamados_processados'] += 1

            if chamado['data_criacao']:
                nova_data = chamado['data_criacao'].strftime('%Y-%m-%d %H:%M:%S')
                self.ultima_data_processada = nova_data
                self.salvar_ultima_data(nova_data)

            if i < len(chamados_realmente_novos):
                time.sleep(0.5)

        self.log_detalhado(f"🎉 Processamento resiliente concluído!", 'SUCCESS')

    def carregar_chamados_enviados(self):
        """Carrega a lista de chamados que já foram enviados pro WhatsApp"""
        try:
            if os.path.exists('chamados_enviados.json'):
                with open('chamados_enviados.json', 'r') as f:
                    chamados = set(json.load(f))
                    self.log_detalhado(f"📂 Carregados {len(chamados)} chamados da lista de enviados", 'DEBUG')
                    return chamados
            return set()
        except Exception as e:
            self.log_detalhado(f"⚠️ Erro ao carregar lista de enviados: {e}", 'WARNING')
            return set()

    def salvar_chamados_enviados(self):
        """Salva a lista de chamados enviados"""
        try:
            with open('chamados_enviados.json', 'w') as f:
                json.dump(list(self.chamados_enviados), f)
            self.log_detalhado(f"💾 Lista de enviados salva ({len(self.chamados_enviados)} itens)", 'DEBUG')
        except Exception as e:
            self.log_detalhado(f"❌ Erro ao salvar lista de enviados: {e}", 'ERROR')

    def marcar_chamado_como_enviado(self, numero_chamado):
        """Marca um chamado como já enviado"""
        self.chamados_enviados.add(str(numero_chamado))
        self.salvar_chamados_enviados()
        self.log_detalhado(f"✅ Chamado {numero_chamado} marcado como enviado", 'SUCCESS')

    def processar_chamado_individual(self, chamado):
        """Processa um chamado específico - COM LOGS DETALHADOS"""
        self.log_detalhado("=" * 60, 'INFO')
        self.log_detalhado(f"🆕 NOVO CHAMADO DETECTADO!", 'SUCCESS')
        self.log_detalhado("=" * 60, 'INFO')
        self.log_detalhado(f"📋 Número: {chamado['numero_chamado']}", 'INFO')
        self.log_detalhado(f"🔧 Tipo: {chamado['tipo_servico']}", 'INFO')
        self.log_detalhado(f"👤 Solicitante: {chamado['solicitante']}", 'INFO')
        self.log_detalhado(f"📍 Local: {chamado['local']}", 'INFO')
        self.log_detalhado(
            f"📝 Descrição: {chamado['descricao_chamado'][:100]}{'...' if len(str(chamado['descricao_chamado'])) > 100 else ''}",
            'INFO')
        self.log_detalhado(f"🕐 Criado em: {chamado['data_criacao']}", 'INFO')
        if chamado['emergencial']:
            self.log_detalhado(f"🚨 EMERGENCIAL: SIM", 'WARNING')
        self.log_detalhado("=" * 60, 'INFO')

        # INTEGRAÇÃO WHATSAPP
        if hasattr(self, 'whatsapp_sender') and self.whatsapp_sender:
            self.log_detalhado("📱 Enviando para WhatsApp...", 'INFO')
            inicio_whatsapp = time.time()

            try:
                sucesso = self.whatsapp_sender.enviar_chamado(chamado)
                tempo_whatsapp = time.time() - inicio_whatsapp

                if sucesso:
                    self.log_detalhado(f"✅ WhatsApp enviado com sucesso em {tempo_whatsapp:.2f}s!", 'SUCCESS')
                else:
                    self.log_detalhado(f"⚠️ Falha no WhatsApp após {tempo_whatsapp:.2f}s, mas chamado processado",
                                       'WARNING')
            except Exception as e:
                tempo_whatsapp = time.time() - inicio_whatsapp
                self.log_detalhado(f"❌ Erro no WhatsApp após {tempo_whatsapp:.2f}s: {e}", 'ERROR')
        else:
            self.log_detalhado("📱 WhatsApp não configurado - salvando apenas localmente", 'WARNING')

    def get_ultima_data_salva(self):
        """Recupera a última data processada com logs"""
        data_padrao = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

        try:
            if os.path.exists('ultima_data_mercedes.txt'):
                with open('ultima_data_mercedes.txt', 'r') as f:
                    data_str = f.read().strip()

                # Verifica se parece com UUID (dados corrompidos)
                if len(data_str) > 30 or '-' in data_str[:8]:
                    self.log_detalhado(f"⚠️ UUID detectado no arquivo de data: {data_str[:30]}... Resetando!",
                                       'WARNING')
                    os.remove('ultima_data_mercedes.txt')
                    self.salvar_ultima_data(data_padrao)
                    return data_padrao

                # Verifica se é data válida
                try:
                    datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')
                    self.log_detalhado(f"📅 Data carregada do arquivo: {data_str}", 'DEBUG')
                    return data_str
                except:
                    self.log_detalhado("⚠️ Data inválida no arquivo, usando padrão", 'WARNING')
                    self.salvar_ultima_data(data_padrao)
                    return data_padrao
            else:
                self.log_detalhado("📅 Arquivo de data não existe, criando com data padrão", 'INFO')
                self.salvar_ultima_data(data_padrao)

        except Exception as e:
            self.log_detalhado(f"❌ Erro ao ler arquivo de data: {e}", 'ERROR')

        return data_padrao

    def salvar_ultima_data(self, data_processada):
        """Salva data com validação e logs"""
        try:
            if isinstance(data_processada, datetime):
                data_str = data_processada.strftime('%Y-%m-%d %H:%M:%S')
            else:
                data_str = str(data_processada)

            # Validação anti-UUID
            if len(data_str) > 30 or (len(data_str) > 20 and '-' in data_str[:8]):
                self.log_detalhado(f"🚨 Tentativa de salvar UUID como data: {data_str[:30]}... BLOQUEADO!", 'ERROR')
                return

            # Testa se consegue parsear
            datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')

            with open('ultima_data_mercedes.txt', 'w') as f:
                f.write(data_str)

            self.log_detalhado(f"💾 Data salva com sucesso: {data_str}", 'DEBUG')

        except Exception as e:
            self.log_detalhado(f"❌ ERRO ao salvar data: {e} | Valor: {data_processada}", 'ERROR')

    def testar_conexao_rapida_resiliente(self):
        """
        Teste rápido mas que detecta SSL EOF e ativa berserker se necessário
        """
        try:
            self.log_detalhado("🔍 Teste rápido de conexão...", 'DEBUG')

            # Primeira tentativa normal
            conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port'],
                connect_timeout=10,  # Timeout bem baixo pro teste rápido
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=5,
                keepalives_count=5,
                sslmode='prefer'
            )

            # Testa com query simples
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()

            self.log_detalhado("✅ Teste rápido passou!", 'SUCCESS')
            return True

        except Exception as e:
            erro_str = str(e)
            self.log_detalhado(f"❌ Teste rápido falhou: {erro_str[:100]}...", 'ERROR')

            # Se for SSL EOF, não considera como falha "normal"
            # Isso vai fazer o sistema entrar no berserker no próximo ciclo
            if self.detectar_erro_ssl_eof(erro_str):
                self.log_detalhado("🔥 SSL EOF no teste! Sistema vai entrar em berserker no próximo ciclo", 'WARNING')
                return False

            return False

    def testar_conexao_super_rapida(self):
        """
        Versão ultra básica só pra ver se o servidor responde
        Usado quando o sistema tá muito instável
        """
        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port'],
                connect_timeout=5,  # Super rápido
                sslmode='prefer'
            )
            conn.close()
            return True
        except:
            return False

    def rodar_monitor(self):
        """Loop principal BERSERKER com detecção inteligente de SSL EOF"""
        self.log_detalhado("🚀 MONITOR MERCEDES MODO BERSERKER INICIANDO...", 'SUCCESS')
        self.log_detalhado(f"🏢 Servidor: {self.db_config['host']}", 'INFO')
        self.log_detalhado(f"💽 Database: {self.db_config['database']}", 'INFO')
        self.log_detalhado(f"🔥 MODO BERSERKER SSL EOF DETECTOR ATIVO!", 'SUCCESS')
        self.log_detalhado("-" * 50, 'INFO')

        contador_ciclos = 0
        contador_stats = 0

        while True:
            try:
                contador_ciclos += 1
                contador_stats += 1

                self.log_detalhado(f"🔄 Ciclo #{contador_ciclos} iniciado", 'INFO')

                # Mostra stats a cada 10 ciclos
                if contador_stats >= 10:
                    self.mostrar_stats_sessao()
                    contador_stats = 0

                # Testa conexão com detector de SSL EOF
                if not self.testar_conexao_rapida_resiliente():
                    self.log_detalhado("🚨 Banco indisponível!", 'ERROR')
                    self.conexoes_falharam_consecutivas += 1

                    # AQUI É O DIFERENCIAL - Se for SSL EOF, não espera muito!
                    if self.conexoes_falharam_consecutivas <= 2:
                        tempo_espera = 30  # Só 30s nas primeiras falhas
                        self.log_detalhado(
                            f"⏳ Falha inicial - aguardando apenas {tempo_espera}s (berserker vai ativar se for SSL EOF)...",
                            'WARNING')
                    elif self.conexoes_falharam_consecutivas <= 5:
                        tempo_espera = 60  # 1 minuto se persistir
                        self.log_detalhado(f"⏳ Persistindo - aguardando {tempo_espera}s...", 'WARNING')
                    else:
                        tempo_espera = 120  # Máximo 2 minutos (não 5!)
                        self.log_detalhado(f"🚨 Sistema instável - aguardando {tempo_espera}s...", 'ERROR')

                    time.sleep(tempo_espera)
                    continue

                # Reset contador se conectou
                if self.conexoes_falharam_consecutivas > 0:
                    self.log_detalhado(f"✅ Conexão restaurada após {self.conexoes_falharam_consecutivas} falha(s)!",
                                       'SUCCESS')
                    self.conexoes_falharam_consecutivas = 0

                # Processa chamados com MODO BERSERKER
                try:
                    # USA A VERSÃO RESILIENTE QUE TEM BERSERKER!
                    self.processar_novos_chamados_resiliente()
                except Exception as e:
                    erro_str = str(e)
                    self.log_detalhado(f"💥 Erro ao processar: {erro_str[:100]}...", 'ERROR')

                    # Se for SSL EOF, não conta como falha normal
                    if self.detectar_erro_ssl_eof(erro_str):
                        self.log_detalhado("🔥 SSL EOF detectado! Berserker já deve ter ativado", 'WARNING')
                    else:
                        self.conexoes_falharam_consecutivas += 1

                # Intervalo INTELIGENTE baseado na performance
                if self.stats_sessao['consultas_realizadas'] > 0:
                    tempo_medio = self.stats_sessao['tempo_total_consultas'] / self.stats_sessao['consultas_realizadas']

                    # Intervalos menores porque o berserker resolve SSL EOF rapidinho
                    if tempo_medio > 15:  # Muito lento
                        intervalo = 150  # 2.5 minutos
                        self.log_detalhado(f"🐌 Sistema lento (média: {tempo_medio:.1f}s) - intervalo 2.5min", 'WARNING')
                    elif tempo_medio > 8:  # Médio
                        intervalo = 120  # 2 minutos
                        self.log_detalhado(f"⚡ Sistema médio (média: {tempo_medio:.1f}s) - intervalo 2min", 'INFO')
                    else:  # Rápido
                        intervalo = 90  # 1.5 minutos (mais agressivo!)
                        self.log_detalhado(f"🚀 Sistema rápido (média: {tempo_medio:.1f}s) - intervalo 1.5min",
                                           'SUCCESS')
                else:
                    intervalo = 120  # Default

                self.log_detalhado(f"😴 Dormindo por {intervalo}s...", 'INFO')
                time.sleep(intervalo)

            except KeyboardInterrupt:
                self.log_detalhado("\n👋 Monitor parado pelo usuário (Ctrl+C)", 'INFO')
                self.mostrar_stats_sessao()
                break

            except Exception as e:
                erro_str = str(e)
                self.log_detalhado(f"💥 Erro inesperado: {erro_str[:100]}...", 'ERROR')

                # Se for SSL EOF, recuperação mais rápida
                if self.detectar_erro_ssl_eof(erro_str):
                    self.log_detalhado("🔥 SSL EOF no loop principal! Recuperação rápida...", 'WARNING')
                    tempo_espera = 30  # Só 30s para SSL EOF
                else:
                    # Outros erros, recuperação normal
                    self.conexoes_falharam_consecutivas += 1
                    tempo_espera = min(60 * self.conexoes_falharam_consecutivas, 180)  # Máximo 3min

                self.log_detalhado(f"⏳ Recuperando em {tempo_espera}s...", 'WARNING')
                time.sleep(tempo_espera)

# Teste direto
if __name__ == "__main__":
    monitor = MonitorChamadosMercedes()
    monitor.rodar_monitor()