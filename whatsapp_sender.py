import requests
import json
import time
from datetime import datetime
import urllib3

# Desabilita os warnings chatos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class WhatsAppSender:
    def __init__(self):
        # COLOQUE SUAS CREDENCIAIS AQUI
        self.api_key = "2712x7347337999"
        self.grupo_id = "120363421436589792@g.us"

        # Configurações automáticas
        self.base_url = f"https://us.api-wa.me/{self.api_key}/message/text"
        self.headers = {
            'Content-Type': 'application/json'
        }

    def enviar_mensagem(self, mensagem, chat_id=None):
        """Envia uma mensagem pro WhatsApp usando sua API"""
        if not chat_id:
            chat_id = self.grupo_id

        # Payload conforme sua documentação
        payload = json.dumps({
            "to": chat_id,
            "text": mensagem
        })

        try:
            # Adicionando verify=False pra ignorar SSL
            response = requests.post(
                self.base_url,
                headers=self.headers,
                data=payload,
                verify=False,  # Isso aqui resolve o problema de SSL
                timeout=30  # E um timeout pra não travar
            )

            print(f"Status da resposta: {response.status_code}")
            print(f"Resposta da API: {response.text}")

            # Verifica se deu certo (pode precisar ajustar dependendo da sua API)
            if response.status_code == 200:
                print("Mensagem enviada com sucesso!")
                return True
            else:
                print(f"Erro ao enviar mensagem: {response.status_code}")
                return False

        except Exception as e:
            print(f"Erro na requisição: {e}")
            return False

    def enviar_chamado(self, dados_chamado):
        """Envia um chamado formatado pro grupo da Mercedes"""
        mensagem = self.formatar_mensagem_chamado(dados_chamado)

        sucesso = self.enviar_mensagem(mensagem)

        if sucesso:
            print(f"Chamado {dados_chamado.get('numero_chamado')} enviado pro WhatsApp!")
            self.salvar_log_envio(dados_chamado)
        else:
            print(f"Falha ao enviar chamado {dados_chamado.get('numero_chamado')}")

        return sucesso

    def formatar_mensagem_chamado(self, chamado):
        """Formata a mensagem do chamado para o WhatsApp no formato solicitado"""
        # Extrai os dados do chamado
        numero = chamado.get('numero_chamado', 'N/A')
        local = chamado.get('local', 'N/A')
        solicitante = chamado.get('solicitante', 'N/A')

        # Agora o telefone tá vindo da query corretamente
        contato = chamado.get('telefone', 'Não informado')
        descricao = chamado.get('descricao_chamado', 'Sem descrição')

        # Campo emergencial que agora tá na query
        emergencial = chamado.get('emergencial', False)

        # Formata a data
        data_formatada = self.formatar_data(chamado.get('data_criacao'))

        # Formata o campo emergencial com emoji e negrito se for urgente
        if emergencial:
            emergencial_texto = "*🚨 SIM 🚨*"
            # Se for emergencial, deixa o título mais destacado também
            titulo = "*🚨 EMERGENCIAL - NOVO CHAMADO - MERCEDES SBC 🚨*"
        else:
            emergencial_texto = "Não"
            titulo = "NOVO CHAMADO - MERCEDES SBC"

        # Monta a mensagem no formato exato que você pediu
        mensagem = f"""{titulo}

Chamado: {numero}
Local: *{local}*
Solicitante: {solicitante}
Contato: {contato}
Descrição:
*{descricao}*
Emergencial: {emergencial_texto}
Criado em: {data_formatada}"""

        return mensagem

    def formatar_data(self, data_criacao):
        """Formata a data pra ficar bonitinha"""
        if data_criacao:
            if isinstance(data_criacao, str):
                try:
                    data_obj = datetime.strptime(data_criacao, '%Y-%m-%d %H:%M:%S')
                    return data_obj.strftime('%d/%m/%Y às %H:%M')
                except:
                    return data_criacao
            else:
                return data_criacao.strftime('%d/%m/%Y às %H:%M')
        return 'Data não informada'

    def salvar_log_envio(self, chamado):
        """Salva um log dos envios realizados"""
        log_entry = {
            'numero_chamado': chamado.get('numero_chamado'),
            'data_envio': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'solicitante': chamado.get('solicitante'),
            'tipo_servico': chamado.get('tipo_servico')
        }

        try:
            try:
                with open('logs_whatsapp.json', 'r') as f:
                    logs = json.load(f)
            except:
                logs = []

            logs.append(log_entry)

            with open('logs_whatsapp.json', 'w') as f:
                json.dump(logs, f, indent=2)

        except Exception as e:
            print(f"Erro ao salvar log: {e}")

    def testar_conexao(self):
        """Testa se a API tá funcionando"""
        print("Testando conexão com API WhatsApp...")

        mensagem_teste = "Teste de conexão - Sistema Mercedes"
        resultado = self.enviar_mensagem(mensagem_teste)

        if resultado:
            print("API funcionando perfeitamente!")
        else:
            print("Problema na API, verifica as configurações")

        return resultado

    def enviar_tarefa_finalizada(self, tarefa_data):
        """Envia notificação de tarefa finalizada pro grupo"""
        mensagem = self.formatar_mensagem_tarefa_finalizada(tarefa_data)
        return self.enviar_mensagem(mensagem)

    def formatar_mensagem_tarefa_finalizada(self, tarefa):
        """Formata mensagem de conclusão"""
        print("DEBUG tarefa finalizada:", tarefa)

        emergencial_emoji = "🚨 " if tarefa.get('emergencial') else ""

        mensagem = f"""{emergencial_emoji}✅ TAREFA CONCLUÍDA - MERCEDES SBC

Chamado: {tarefa.get('numero_chamado', 'N/A')}
Tarefa: {tarefa.get('numero_tarefa', 'N/A')}
Local: {tarefa.get('local', 'N/A')}  
Colaborador: {tarefa.get('colaborador', 'N/A')}
Finalizado em: {self.formatar_data(tarefa.get('data_finalizacao'))}

Trabalho concluído com sucesso!!! """

        return mensagem

# Exemplo de como usar:
if __name__ == "__main__":
    # Cria o sender
    whatsapp = WhatsAppSender()

    # Exemplo de chamado pra testar
    chamado_exemplo = {
        'numero_chamado': '12345',
        'tipo_servico': 'MANUTENÇÃO DE EMPILHADEIRA',
        'solicitante': 'João Silva',
        'local': 'Galpão A - Setor 2',
        'descricao_chamado': 'Empilhadeira com problema no sistema hidráulico',
        'data_criacao': datetime.now(),
        'categoria': 'Manutenção',
        'subcategoria': 'Equipamentos'
    }

    # Testa o formato da mensagem
    print("FORMATO DA MENSAGEM:")
    print("=" * 50)
    print(whatsapp.formatar_mensagem_chamado(chamado_exemplo))
    print("=" * 50)

    # Para testar de verdade:
    # whatsapp.testar_conexao()
    # whatsapp.enviar_chamado(chamado_exemplo)