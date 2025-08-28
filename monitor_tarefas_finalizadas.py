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

        # Data de referência (últimas 48h pra pegar tarefas novas) - DEFINE PRIMEIRO!
        data_referencia = (datetime.now() - timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')

        # Lista os IDs que já estão sendo observados pra não duplicar
        ids_ja_observados = [t['id'] for t in self.tarefas_em_observacao]

        # Monta a query de forma inteligente dependendo se tem IDs ou não
        if ids_ja_observados:
            # Se tem IDs, usa eles na query
            placeholders = ','.join(['%s'] * len(ids_ja_observados))
            where_clause = f"AND T.id NOT IN ({placeholders})"
            params = tuple(ids_ja_observados) + (data_referencia,)
        else:
            # Se não tem IDs, não coloca essa condição
            where_clause = ""
            params = (data_referencia,)

        query = f"""
        SELECT 
            T.id as tarefa_id,
            T.numero as numero_tarefa,
            T.objetoorigemid as chamado_id,
            T.status as status_atual,
            T.criado as data_criacao,
            C.numero as numero_chamado,
          C.emergencial as emergencial,
          C.nome as local
      FROM dbo.tarefa T
      , dbo.chamado C
      WHERE C.id = T.objetoorigemid
        AND T.origem = 48                     -- Só tarefas que vieram de chamado
        AND T.status IN (10, 25)                -- Abertas (10) ou Iniciadas (25)
        AND T.estruturanivel2 IN ('44462 - SP - MAI - MERCEDES - SBC - MANUT')
        {where_clause}                          -- Não pega as que já tão sendo observadas
        AND T.criado > %s::timestamp            -- Só tarefas criadas recentemente
      ORDER BY T.criado DESC
      LIMIT 50                                -- Limita pra não sobrecarregar
      """

        try:
            cursor.execute(query, params)
            novas_tarefas = cursor.fetchall()

            colunas = [desc[0] for desc in cursor.description]
            tarefas_dict = [dict(zip(colunas, linha)) for linha in novas_tarefas]

            cursor.close()
            conn.close()

            return tarefas_dict

        except Exception as e:
            print(f"Erro ao buscar novas tarefas: {e}")
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            return []

    def verificar_status_tarefas_observadas(self):
        """Verifica se alguma tarefa da lista mudou pra finalizada"""
        if not self.tarefas_em_observacao:
            return []

        conn = self.conectar_bd()
        if not conn:
            return []

        cursor = conn.cursor()

        # Pega os IDs das tarefas que estão sendo observadas
        ids_observados = [t['id'] for t in self.tarefas_em_observacao]

        # Cria os placeholders corretamente pra UUIDs
        placeholders = ','.join(['%s'] * len(ids_observados))

        query = f"""
        SELECT 
            T.id as tarefa_id,
            T.numero as numero_tarefa,
            T.status as status_atual,
            T.terminoreal as data_finalizacao,
            T.inicioreal as data_inicio,
              T.servicodescricao as tipo_servico,
              E.rotulo as colaborador,
              C.numero as numero_chamado,
              C.emergencial as emergencial,
              C.nome as local
          FROM dbo.tarefa T
          LEFT JOIN dbo.executor E ON E.tarefaid = T.id,
          dbo.chamado C
          WHERE C.id = T.objetoorigemid
            AND T.id IN ({placeholders})
          """

        try:
            # Passa os UUIDs como parâmetros (o psycopg2 vai tratar eles direito)
            cursor.execute(query, tuple(ids_observados))
            tarefas_atualizadas = cursor.fetchall()

            colunas = [desc[0] for desc in cursor.description]
            tarefas_dict = [dict(zip(colunas, linha)) for linha in tarefas_atualizadas]

            cursor.close()
            conn.close()

            # Filtra só as que mudaram pra finalizada (status 85)
            tarefas_finalizadas = []
            tarefas_ainda_pendentes = []

            for tarefa in tarefas_dict:
                if tarefa['status_atual'] == 85:
                    # Finalizada
                    tarefas_finalizadas.append(tarefa)
                else:
                    # Procura se já existia no JSON
                    tarefa_existente = next(
                        (t for t in self.tarefas_em_observacao if str(t['id']) == str(tarefa['tarefa_id'])), {}
                    )

                    # Atualiza mantendo campos extras
                    tarefa_existente.update({
                        'id': str(tarefa['tarefa_id']),
                        'numero_tarefa': tarefa['numero_tarefa'],
                        'numero_chamado': tarefa['numero_chamado'],
                        'status': tarefa['status_atual']
                    })

                    tarefas_ainda_pendentes.append(tarefa_existente)

            # Só substitui se realmente houver algo, senão mantém
            if tarefas_ainda_pendentes:
                self.tarefas_em_observacao = tarefas_ainda_pendentes

            self.salvar_tarefas_em_observacao()

            # Atualiza a lista de observação (remove as finalizadas)
            self.tarefas_em_observacao = tarefas_ainda_pendentes
            self.salvar_tarefas_em_observacao()

            return tarefas_finalizadas

        except Exception as e:
            print(f"Erro ao verificar status: {e}")
            if cursor:
                cursor.close()
            if conn:
                conn.close()
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
            print(f"Colaborador: {tarefa['colaborador']}")
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