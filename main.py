from monitor_chamados import MonitorChamadosMercedes
from monitor_tarefas_finalizadas import MonitorTarefasFinalizadas
from whatsapp_sender import WhatsAppSender
import threading
import time
import sys


def main():
    print("SISTEMA INTEGRADO MERCEDES - CHAMADOS + TAREFAS + WHATSAPP")
    print("=" * 70)

    try:
        # Inicializa o WhatsApp
        print("Inicializando sistema WhatsApp...")
        whatsapp = WhatsAppSender()

        # Verifica se as credenciais foram configuradas
        if whatsapp.api_key == "COLE_SUA_API_KEY_AQUI" or whatsapp.grupo_id == "COLE_SEU_GRUPO_ID_AQUI":
            print("ERRO: Configure suas credenciais no whatsapp_sender.py primeiro!")
            print("- Edite whatsapp_sender.py")
            print("- Cole sua API_KEY e GRUPO_ID")
            print("- Rode novamente")
            return

        # Testa se o WhatsApp tá funcionando
        print("Testando conexão WhatsApp...")
        if not whatsapp.testar_conexao():
            print("PROBLEMA: WhatsApp não conectou. Verifica suas credenciais!")
            resposta = input("Quer continuar mesmo assim? (s/N): ")
            if resposta.lower() != 's':
                print("Sistema cancelado. Arruma o WhatsApp e tenta de novo!")
                return

        # Inicializa os dois monitors
        print("Inicializando monitor de chamados...")
        monitor_chamados = MonitorChamadosMercedes()

        print("Inicializando monitor de tarefas finalizadas...")
        monitor_tarefas = MonitorTarefasFinalizadas()

        # Conecta os WhatsApp nos dois monitors
        monitor_chamados.whatsapp_sender = whatsapp
        monitor_tarefas.whatsapp_sender = whatsapp

        print("SISTEMAS CONECTADOS COM SUCESSO!")
        print("A partir de agora:")
        print("- Monitor 1: fica de olho em chamados novos")
        print("- Monitor 2: fica de olho em tarefas finalizadas")
        print("- Ambos mandam mensagem automática pro WhatsApp")
        print("=" * 70)

        # Cria as threads para rodar os dois monitors simultaneamente
        print("Iniciando threads dos monitors...")

        thread_chamados = threading.Thread(
            target=monitor_chamados.rodar_monitor,
            name="MonitorChamados"
        )

        thread_tarefas = threading.Thread(
            target=monitor_tarefas.rodar_monitor,
            name="MonitorTarefas"
        )

        # Marca as threads como daemon (param quando o main terminar)
        thread_chamados.daemon = True
        thread_tarefas.daemon = True

        # Inicia as duas threads
        thread_chamados.start()
        thread_tarefas.start()

        print("MONITORS RODANDO EM PARALELO!")
        print("Para parar o sistema: Ctrl+C")
        print("-" * 70)

        # Loop principal que mantém o programa rodando
        try:
            while True:
                time.sleep(10)  # Check a cada 10 segundos se as threads tão vivas

                # Verifica se as threads ainda tão rodando
                if not thread_chamados.is_alive():
                    print("AVISO: Thread de chamados parou!")

                if not thread_tarefas.is_alive():
                    print("AVISO: Thread de tarefas parou!")

        except KeyboardInterrupt:
            print("\nSistema parado pelo usuário (Ctrl+C)")
            print("Finalizando threads...")

    except ImportError as e:
        print(f"ERRO: Não conseguiu importar os módulos: {e}")
        print("Verifica se os arquivos estão na mesma pasta:")
        print("- monitor_chamados.py")
        print("- monitor_tarefas_finalizadas.py")
        print("- whatsapp_sender.py")
        print("- main.py")

    except Exception as e:
        print(f"ERRO INESPERADO: {e}")
        print("Alguma coisa deu errado! Verifica os logs acima.")


def verificar_dependencias():
    """Verifica se todas as dependências estão instaladas"""
    dependencias = ['psycopg2', 'requests', 'threading']
    faltando = []

    for dep in dependencias:
        if dep == 'threading':  # threading é builtin
            continue
        try:
            __import__(dep)
        except ImportError:
            faltando.append(dep)

    if faltando:
        print("FALTAM DEPENDÊNCIAS:")
        for dep in faltando:
            print(f"- {dep}")
        print("\nInstale com: pip install " + " ".join(faltando))
        return False

    return True


if __name__ == "__main__":
    print("MERCEDES SISTEMA AUTOMATICO V2.0 - DUAL MONITOR")
    print("=" * 50)

    # Verifica dependências primeiro
    if not verificar_dependencias():
        sys.exit(1)

    # Roda o sistema principal
    main()