import psycopg2
import time
import json
import os
from datetime import datetime, timedelta


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
        print(f"Última data processada: {self.ultima_data_processada}")
        print(f"Chamados já enviados: {len(self.chamados_enviados)}")

    def carregar_chamados_enviados(self):
        """Carrega a lista de chamados que já foram enviados pro WhatsApp"""
        try:
            if os.path.exists('chamados_enviados.json'):
                with open('chamados_enviados.json', 'r') as f:
                    return set(json.load(f))
            return set()
        except:
            return set()

    def salvar_chamados_enviados(self):
        """Salva a lista de chamados enviados"""
        try:
            with open('chamados_enviados.json', 'w') as f:
                json.dump(list(self.chamados_enviados), f)
        except Exception as e:
            print(f"Erro ao salvar lista de enviados: {e}")

    def marcar_chamado_como_enviado(self, numero_chamado):
        """Marca um chamado como já enviado"""
        self.chamados_enviados.add(str(numero_chamado))
        self.salvar_chamados_enviados()
        print(f"Chamado {numero_chamado} marcado como enviado")

    def conectar_bd(self):
        """Conecta no PostgreSQL da Mercedes"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"Deu ruim na conexão: {e}")
            return None

    def buscar_novos_chamados(self):
        """Pega os chamados novos que ainda não foram processados"""
        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        # Query usando dbo.chamado com FULL JOIN (pega tudo mesmo)
        query = """
                SELECT C.numero              as numero_chamado, \
                       C.id                  as id_chamado, \
                       C.nome                as local, \
                       C.descricao           as descricao_chamado, \
                       C.status              as status_chamado, \
                       C.emergencial         as emergencial, \
                       C.solicitantetelefone as telefone, \
                       T.numero              as numero_tarefa, \
                       T.solicitantenome     as solicitante, \
                       T.estruturahierarquiadescricao as estrutura_hierarquia,
            T.criado as data_criacao,
            T.descricao as descricao_tarefa,
            T.servicodescricao as tipo_servico,
            dms.servico,
            dms.subcategoria,
            dms.categoria
                FROM dbo.chamado C
                    FULL JOIN dbo.tarefa T \
                ON T.objetoorigemid = C.id,
                    dw_vista.dm_servico dms
                WHERE dms.id_servico = T.servicoid
                  AND T.estruturanivel2 IN ('44462 - SP - MAI - MERCEDES - SBC - MANUT')
                  AND T.origem = 48
                  AND T.criado \
                    > %s:: timestamp
                ORDER BY T.criado \
                """

        try:
            cursor.execute(query, (self.ultima_data_processada,))
            novos = cursor.fetchall()

            # Converte pra dicionário pra ficar mais fácil de usar
            colunas = [desc[0] for desc in cursor.description]
            chamados = [dict(zip(colunas, linha)) for linha in novos]

            cursor.close()
            conn.close()

            return chamados

        except Exception as e:
            print(f"Erro na consulta: {e}")
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            return []

    def processar_novos_chamados(self):
        """Processa cada chamado novo que apareceu"""
        novos_chamados = self.buscar_novos_chamados()

        if not novos_chamados:
            print("Nenhum chamado novo por enquanto...")
            return

        print(f"Encontrei {len(novos_chamados)} chamado(s) no banco...")

        # Filtra apenas os que REALMENTE são novos (não foram enviados ainda)
        chamados_realmente_novos = []
        for chamado in novos_chamados:
            numero_chamado = str(chamado['numero_chamado'])
            if numero_chamado not in self.chamados_enviados:
                chamados_realmente_novos.append(chamado)
            else:
                print(f"Chamado {numero_chamado} já foi enviado antes")

        if not chamados_realmente_novos:
            print("Todos os chamados já foram processados anteriormente")
            return

        print(f"{len(chamados_realmente_novos)} chamado(s) REALMENTE NOVOS!")

        for chamado in chamados_realmente_novos:
            # Processa o chamado individual
            self.processar_chamado_individual(chamado)

            # IMPORTANTE: Marca como enviado para não repetir
            self.marcar_chamado_como_enviado(chamado['numero_chamado'])

            # Atualiza a última data processada
            if chamado['data_criacao']:
                self.ultima_data_processada = chamado['data_criacao'].strftime('%Y-%m-%d %H:%M:%S')
                self.salvar_ultima_data(self.ultima_data_processada)

    def processar_chamado_individual(self, chamado):
        """Processa um chamado específico - agora envia pro WhatsApp também"""
        print("=" * 60)
        print(f"NOVO CHAMADO DETECTADO!")
        print("=" * 60)
        print(f"Número: {chamado['numero_chamado']}")
        print(f"Tipo: {chamado['tipo_servico']}")
        print(f"Solicitante: {chamado['solicitante']}")
        print(f"Local: {chamado['local']}")
        print(f"Descrição: {chamado['descricao_chamado']}")
        print(f"Criado em: {chamado['data_criacao']}")
        print(f"Categoria: {chamado['categoria']}")
        print(f"Subcategoria: {chamado['subcategoria']}")
        print("=" * 60)

        # INTEGRAÇÃO WHATSAPP - A MÁGICA ACONTECE AQUI!
        if hasattr(self, 'whatsapp_sender') and self.whatsapp_sender:
            print("Enviando pro WhatsApp...")
            try:
                sucesso = self.whatsapp_sender.enviar_chamado(chamado)
                if sucesso:
                    print("Chamado enviado pro grupo da Mercedes com sucesso!")
                else:
                    print("Falha ao enviar pro WhatsApp, mas chamado foi processado")
            except Exception as e:
                print(f"Erro ao enviar WhatsApp: {e}")
                print("Continuando mesmo assim...")
        else:
            print("WhatsApp não configurado, só salvando localmente")

        print("=" * 60)

    def formatar_mensagem_whatsapp(self, chamado):
        """Formata a mensagem que vai pro WhatsApp"""
        mensagem = f"""*NOVO CHAMADO - MERCEDES SBC*

*Chamado:* {chamado['numero_chamado']}
*Tipo:* {chamado['tipo_servico']}
*Solicitante:* {chamado['solicitante']}
*Local:* {chamado['local']}

*Descrição:*
{chamado['descricao_chamado']}

*Criado em:* {chamado['data_criacao'].strftime('%d/%m/%Y às %H:%M')}
*Categoria:* {chamado['categoria']} > {chamado['subcategoria']}

_Sistema automático Mercedes - GPS Vista_"""

        return mensagem

    def get_ultima_data_salva(self):
        """Recupera a última data processada de um arquivo"""
        try:
            with open('ultima_data_mercedes.txt', 'r') as f:
                data_str = f.read().strip()
                return data_str if data_str else '2024-01-01 00:00:00'
        except:
            # Se não tem arquivo, pega só os chamados das últimas 24h
            ontem = datetime.now() - timedelta(hours=24)
            return ontem.strftime('%Y-%m-%d %H:%M:%S')

    def salvar_ultima_data(self, data_processada):
        """Salva a última data processada"""
        with open('ultima_data_mercedes.txt', 'w') as f:
            f.write(str(data_processada))
        print(f"Última data salva: {data_processada}")

    def testar_conexao(self):
        """Testa se a conexão tá funcionando"""
        print("Testando conexão...")
        conn = self.conectar_bd()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            versao = cursor.fetchone()
            print(f"PostgreSQL conectado: {versao[0]}")
            cursor.close()
            conn.close()
            return True
        return False

    def rodar_monitor(self):
        """Loop principal que fica rodando forever"""
        print("MONITOR MERCEDES INICIANDO...")
        print(f"Servidor: {self.db_config['host']}")
        print(f"Database: {self.db_config['database']}")
        print(f"Usuário: {self.db_config['user']}")

        # Testa conexão primeiro
        if not self.testar_conexao():
            print("Não conseguiu conectar. Verifica usuário/senha!")
            return

        print("Tudo certo! Monitor rodando...")
        print("Para parar: Ctrl+C")
        print("-" * 50)

        contador_ciclos = 0

        while True:
            try:
                contador_ciclos += 1
                print(f"Ciclo #{contador_ciclos} - {datetime.now().strftime('%H:%M:%S')}")

                self.processar_novos_chamados()

                print("Dormindo 120 segundos...")
                time.sleep(120)

            except KeyboardInterrupt:
                print("\nMonitor parado pelo usuário")
                break

            except Exception as e:
                print(f"Erro inesperado: {e}")
                print("Tentando novamente em 60 segundos...")
                time.sleep(60)


# Como usar:
if __name__ == "__main__":
    print("MONITOR DE CHAMADOS MERCEDES")
    print("=" * 40)
    print("Usuário: gpssa_pg_jonatan_lopes")
    print("Conectando na Mercedes SBC...")

    monitor = MonitorChamadosMercedes()
    monitor.rodar_monitor()