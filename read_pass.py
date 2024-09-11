with open('password.txt') as f:
    lines = [line.rstrip() for line in f]
    
username = lines[0]
pg_password = lines[1]

print(username,pg_password)

