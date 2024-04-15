data_path = 'Data/Input/points.txt'

# data input done
points = []
with open(data_path, 'r') as f:
    lines = f.readlines()
    
for line in lines:
    pt = list(map(float, line.strip().split(',')))
    points.append(pt)


