import psycopg2
import time
import json
import os
from datetime import datetime, timedelta


class MonitorTarefasFinalizadas:
    def __init__(self):
        # Conexão com seu PostgreSQL da Mercedes (mesmas credenciais)
        self.db_config = {
            'host': '10.84.224.17',
            'database': 'dw_gps',
            'user': 'gpssa_pg_jonatan_lopes',
            'password': 'rrxD&!C2qU1V',
            'port': 5432
        }

        # Lista de tarefas que estão sendo monitoradas (pendentes)
        self.tarefas_em_observacao = self.carregar_tarefas_em_observacao()
        self.tarefas_enviadas = self.carregar_tarefas_enviadas()

        print(f"Tarefas sendo observadas: {len(self.tarefas_em_observacao)}")
        print(f"Tarefas já finalizadas: {len(self.tarefas_enviadas)}")

        # WhatsApp será injetado pelo main
        self.whatsapp_sender = None

    def carregar_tarefas_em_observacao(self):
        """Carrega a lista de tarefas que estão sendo observadas"""
        try:
            if os.path.exists('tarefas_observacao.json'):
                with open('tarefas_observacao.json', 'r') as f:
                    return json.load(f)
            return []
        except:
            return []

    def salvar_tarefas_em_observacao(self):
        """Salva a lista de tarefas em observação"""
        try:
            with open('tarefas_observacao.json', 'w', encoding='utf-8') as f:
                json.dump(self.tarefas_em_observacao, f, indent=2, ensure_ascii=False)
            print(f" Salvo {len(self.tarefas_em_observacao)} tarefas em observação no JSON.")
        except Exception as e:
            print(f" Erro ao salvar lista de observação: {e}")

    def carregar_tarefas_enviadas(self):
        """Carrega a lista de tarefas que já foram enviadas pro WhatsApp"""
        try:
            if os.path.exists('tarefas_finalizadas_enviadas.json'):
                with open('tarefas_finalizadas_enviadas.json', 'r') as f:
                    return set(json.load(f))
            return set()
        except:
            return set()

    def salvar_tarefas_enviadas(self):
        """Salva a lista de tarefas enviadas"""
        try:
            with open('tarefas_finalizadas_enviadas.json', 'w') as f:
                json.dump(list(self.tarefas_enviadas), f)
        except Exception as e:
            print(f"Erro ao salvar lista de enviadas: {e}")

    def conectar_bd(self):
        """Conecta no PostgreSQL da Mercedes"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            print(f"Erro na conexão: {e}")
            return None

    def buscar_novas_tarefas_pendentes(self):
        """Pega tarefas novas que vieram de chamado e ainda não estão finalizadas"""
        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        # Data de referência (últimas 48h)
        data_referencia = (datetime.now() - timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')

        # Query simples só das tarefas pendentes
        query = """
                SELECT T.id             as tarefa_id, \
                       T.numero         as numero_tarefa, \
                       T.objetoorigemid as chamado_id, \
                       T.status         as status_atual, \
                       T.criado         as data_criacao
                FROM dbo.tarefa T
                WHERE T.origem = 48
                  AND T.status IN (10, 25)
                  AND T.estruturanivel2 IN ('44462 - SP - MAI - MERCEDES - SBC - MANUT')
                  AND T.criado > %s::timestamp
                ORDER BY T.criado DESC
                    LIMIT 50 \
                """

        try:
            cursor.execute(query, (data_referencia,))
            tarefas_raw = cursor.fetchall()
            cursor.close()
            conn.close()

            if not tarefas_raw:
                return []

            # Converte pra dict e filtra as já observadas
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

            # Se tem tarefas novas, busca dados dos chamados
            if tarefas_novas:
                ids_chamados = [t['chamado_id'] for t in tarefas_novas]
                dados_chamados = self.buscar_dados_chamados_por_id(ids_chamados)
                return self.mesclar_tarefas_com_chamados(tarefas_novas, dados_chamados)

            return []

        except Exception as e:
            print(f"Erro ao buscar tarefas: {e}")
            return []

    def buscar_dados_chamados_por_id(self, ids_chamados):
        """Query separada pros dados dos chamados"""
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
            C.emergencial
        FROM dbo.chamado C
        WHERE C.id IN ({placeholders})
        """

        try:
            cursor.execute(query, tuple(ids_chamados))
            chamados_raw = cursor.fetchall()
            cursor.close()
            conn.close()

            # Retorna como dict indexado pelo ID
            return {c[0]: {'numero_chamado': c[1], 'local': c[2], 'emergencial': c[3]} for c in chamados_raw}

        except Exception as e:
            print(f"Erro ao buscar chamados: {e}")
            return {}

    def mesclar_tarefas_com_chamados(self, tarefas, dados_chamados):
        """Junta dados das duas queries"""
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

        return resultado

    def verificar_status_tarefas_observadas(self):
        """Verifica se alguma tarefa da lista mudou pra finalizada"""
        if not self.tarefas_em_observacao:
            return []

        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        # Query simples só pra checar status
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

            for t in tarefas_raw:
                if t[2] == 85:  # status_atual == 85
                    ids_finalizadas.append(t[0])  # tarefa_id
                else:
                    # Mantém na observação
                    tarefa_obs = {
                        'id': str(t[0]),
                        'numero_tarefa': t[1],
                        'status': t[2]
                    }
                    tarefas_ainda_pendentes.append(tarefa_obs)

            # Atualiza lista de observação
            self.tarefas_em_observacao = tarefas_ainda_pendentes
            self.salvar_tarefas_em_observacao()

            # Busca dados completos das finalizadas
            if ids_finalizadas:
                return self.buscar_dados_tarefa_finalizada_lista(ids_finalizadas)

            return []

        except Exception as e:
            print(f"Erro ao verificar status: {e}")
            return []

    def buscar_dados_tarefa_finalizada_lista(self, ids_tarefas):
        """Busca dados completos das tarefas finalizadas - AGORA COM QUEM REALIZOU!"""
        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(ids_tarefas))

        # AQUI ESTÁ A MÁGICA! Adicionei o JOIN com dbo.recurso
        query = f"""
        SELECT 
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
            cursor.close()
            conn.close()

            resultado = []
            for t in tarefas_raw:
                tarefa_dict = {
                    'numero_tarefa': t[0],
                    'data_finalizacao': t[1],
                    'numero_chamado': t[2],
                    'local': t[3],
                    'emergencial': t[4],
                    'realizador_nome': t[5] or 'Não informado'  # Só o nome, sem email
                }
                resultado.append(tarefa_dict)

            return resultado

        except Exception as e:
            print(f"Erro ao buscar dados finalizadas: {e}")
            return []

    def adicionar_novas_tarefas_observacao(self):
        """Adiciona novas tarefas na lista de observação"""
        novas_tarefas = self.buscar_novas_tarefas_pendentes()

        if novas_tarefas:
            print(f"Encontradas {len(novas_tarefas)} novas tarefas para observar")

            for tarefa in novas_tarefas:
                tarefa_observacao = {
                    'id': tarefa['tarefa_id'],
                    'numero_tarefa': tarefa['numero_tarefa'],
                    'numero_chamado': tarefa['numero_chamado'],
                    'status': tarefa['status_atual'],
                    'emergencial': tarefa['emergencial'],
                    'data_adicao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                self.tarefas_em_observacao.append(tarefa_observacao)
                print(
                    f"Adicionada tarefa {tarefa['numero_tarefa']} do chamado {tarefa['numero_chamado']} na observação")

            self.salvar_tarefas_em_observacao()

    def processar_tarefas_finalizadas(self, tarefas_finalizadas):
        """Processa as tarefas que foram finalizadas"""
        for tarefa in tarefas_finalizadas:
            numero_tarefa = str(tarefa['numero_tarefa'])

            # Verifica se já foi enviada antes
            if numero_tarefa in self.tarefas_enviadas:
                continue

            print("=" * 60)
            print(f"TAREFA FINALIZADA DETECTADA!")
            print("=" * 60)
            print(f"Chamado: {tarefa['numero_chamado']}")
            print(f"Tarefa: {tarefa['numero_tarefa']}")
            print(f"Realizada por: {tarefa['realizador_nome']}")  # NOVA INFO!
            print(f"Finalizada em: {tarefa['data_finalizacao']}")
            print("=" * 60)

            # Envia pro WhatsApp
            if hasattr(self, 'whatsapp_sender') and self.whatsapp_sender:
                try:
                    sucesso = self.whatsapp_sender.enviar_tarefa_finalizada(tarefa)
                    if sucesso:
                        print("Conclusão enviada pro grupo da Mercedes!")
                        self.tarefas_enviadas.add(numero_tarefa)
                        self.salvar_tarefas_enviadas()
                    else:
                        print("Falha no envio, mas tarefa foi processada")
                except Exception as e:
                    print(f"Erro no WhatsApp: {e}")
            else:
                print("WhatsApp não configurado")

            print("=" * 60)

    def rodar_monitor(self):
        """Loop principal - lógica inteligente de observação"""
        print("MONITOR INTELIGENTE DE TAREFAS INICIANDO...")
        print("Lógica: Observa tarefas pendentes até elas serem finalizadas")
        print("-" * 50)

        contador_ciclos = 0

        while True:
            try:
                contador_ciclos += 1
                print(f"Ciclo #{contador_ciclos} - {datetime.now().strftime('%H:%M:%S')}")

                # Etapa 1: Adiciona novas tarefas na lista de observação
                self.adicionar_novas_tarefas_observacao()

                # Etapa 2: Verifica se alguma tarefa observada foi finalizada
                tarefas_finalizadas = self.verificar_status_tarefas_observadas()

                if tarefas_finalizadas:
                    print(f"ATENÇÃO: {len(tarefas_finalizadas)} tarefa(s) foram finalizadas!")
                    self.processar_tarefas_finalizadas(tarefas_finalizadas)
                else:
                    print(f"Observando {len(self.tarefas_em_observacao)} tarefa(s) pendente(s)...")

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
    print("MONITOR INTELIGENTE DE TAREFAS - MERCEDES")
    print("=" * 50)

    monitor = MonitorTarefasFinalizadas()
    monitor.rodar_monitor()