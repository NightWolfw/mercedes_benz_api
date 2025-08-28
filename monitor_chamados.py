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
        """Versão otimizada sem subselects assassinos"""
        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        # Query simples e direta - sem frescura
        query = """
                SELECT T.numero           AS numero_tarefa, \
                       T.criado           AS data_criacao, \
                       T.solicitantenome  AS solicitante, \
                       T.servicodescricao AS tipo_servico, \
                       T.descricao        AS descricao_tarefa, \
                       T.objetoorigemid   AS chamado_id
                FROM dbo.tarefa T
                WHERE T.estruturanivel2 IN ('44462 - SP - MAI - MERCEDES - SBC - MANUT')
                  AND T.origem = 48
                  AND T.criado > %s::timestamp
                ORDER BY T.criado DESC
                    LIMIT 50 \
                """

        try:
            cursor.execute(query, (self.ultima_data_processada,))
            tarefas_raw = cursor.fetchall()
            cursor.close()
            conn.close()

            if not tarefas_raw:
                return []

            # Busca dados dos chamados em query separada
            ids_chamados = [t[5] for t in tarefas_raw]  # t[5] = chamado_id
            dados_chamados = self.buscar_dados_chamados(ids_chamados)

            # Junta os dados das duas consultas
            resultado = []
            for t in tarefas_raw:
                chamado_data = dados_chamados.get(t[5], {})  # t[5] = chamado_id

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

            return resultado

        except Exception as e:
            print(f"Erro na consulta: {e}")
            return []

    def buscar_dados_chamados(self, ids_chamados):
        """Busca dados dos chamados em separado"""
        if not ids_chamados:
            return {}

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

            # Retorna como dict indexado pelo ID do chamado
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
            print(f"Erro ao buscar dados dos chamados: {e}")
            return {}

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
        # REMOVE ESSAS DUAS LINHAS QUE ESTÃO DANDO ERRO:
        # print(f"Categoria: {chamado['categoria']}")
        # print(f"Subcategoria: {chamado['subcategoria']}")
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

"""

        return mensagem

    def get_ultima_data_salva(self):
        """Recupera a última data processada - versão à prova de UUID"""
        # FORÇA um reset se tiver dados corrompidos
        data_padrao = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

        try:
            if os.path.exists('ultima_data_mercedes.txt'):
                with open('ultima_data_mercedes.txt', 'r') as f:
                    data_str = f.read().strip()

                # Verifica se parece com UUID (tem hífens e é longo)
                if len(data_str) > 30 or '-' in data_str[:8]:
                    print(f"DETECTADO UUID NO ARQUIVO: {data_str[:30]}...")
                    print("Resetando para data válida")
                    os.remove('ultima_data_mercedes.txt')  # Deleta arquivo corrompido
                    return data_padrao

                # Verifica se é data válida
                try:
                    datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')
                    return data_str
                except:
                    print("Data inválida no arquivo, resetando")
                    return data_padrao

        except Exception as e:
            print(f"Erro ao ler arquivo: {e}")

        # Salva a data padrão
        with open('ultima_data_mercedes.txt', 'w') as f:
            f.write(data_padrao)

        return data_padrao

    def salvar_ultima_data(self, data_processada):
        """Salva data com validação robusta"""
        try:
            # Converte datetime object pra string se necessário
            if isinstance(data_processada, datetime):
                data_str = data_processada.strftime('%Y-%m-%d %H:%M:%S')
            else:
                data_str = str(data_processada)

            # PARANOIA: verifica se parece com UUID
            if len(data_str) > 30 or (len(data_str) > 20 and '-' in data_str[:8]):
                print(f"AVISO: Tentativa de salvar UUID como data: {data_str[:30]}...")
                print("Ignorando e mantendo data anterior")
                return

            # Testa se consegue parsear
            datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')

            with open('ultima_data_mercedes.txt', 'w') as f:
                f.write(data_str)
            print(f"Data salva: {data_str}")

        except Exception as e:
            print(f"ERRO ao salvar data: {e}")
            print(f"Valor recebido: {data_processada}")
            print("Mantendo data anterior por segurança")

    def salvar_ultima_data(self, data_processada):
        """Salva a última data processada com validação"""
        try:
            # Se data_processada for datetime object, converte
            if isinstance(data_processada, datetime):
                data_str = data_processada.strftime('%Y-%m-%d %H:%M:%S')
            else:
                data_str = str(data_processada)

            # Valida se é uma data válida antes de salvar
            datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')

            with open('ultima_data_mercedes.txt', 'w') as f:
                f.write(data_str)
            print(f"Data salva com sucesso: {data_str}")

        except Exception as e:
            print(f"Erro ao salvar data: {e}")
            print(f"Tentou salvar: {data_processada}")

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