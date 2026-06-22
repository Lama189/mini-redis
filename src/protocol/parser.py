import io

def parse_resp(stream: io.BytesIO):
    prefix = stream.read(1)
    if not prefix:
        return None
    
    line = stream.readline()
    payload = line[:-2]

    if prefix == b'*':
        count = int(payload)
        if count == -1:
            return None
        
        return [parse_resp(stream) for _ in range(count)]
        
    elif prefix == b'$':
        length = int(payload)
        if length == -1:
            return None
        
        data = stream.read(length)
        stream.read(2)

        return data.decode('utf-8')
    
    elif prefix == b'+':
        return payload.decode('utf-8')

    elif prefix == b'-':
        raise Exception(f"Mini-Redis Error: {payload.decode('utf-8')}")
    
    elif prefix == b':':
        return int(payload)

    else:
        raise ValueError(f"Unknown RESP prefix: {prefix}")
        
    
