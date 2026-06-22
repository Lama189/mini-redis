import asyncio
import io

from src.protocol.parser import parse_resp
from src.services.command_dispatcher import dispatch_command


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    print(f"[*] Новое подключение от {addr}")

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                print(f"[-] Клиент {addr} отключился")
                break
            
            buffer = io.BytesIO(data)
            command_parts = parse_resp(buffer)

            print(f"[<-] Получено от {addr}: {command_parts}")

            if isinstance(command_parts, list) and len(command_parts) > 0:
                response_str = await dispatch_command(command_parts)
                response_bytes = response_str.encode('utf-8')
                
                writer.write(response_bytes)
                await writer.drain()
                print(f"[->] Отправлено для {addr}: {response_str.strip()}")
                continue 

            error_response = b"-ERR unknown command\r\n"
            writer.write(error_response)
            await writer.drain()
    
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[!] Ошибка при работе с клиентом {addr}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"[*] Соединение с {addr} полностью закрыто.")
        
                


