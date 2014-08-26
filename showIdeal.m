function showIdeal(filename)
data=load(filename);
xa=data(:,1);
ya=data(:,2);
ca=data(:,3);
nc = size(unique(ca), 1);
hold on;
caxis([1,nc]);
n = size(xa,1);
for i = 1 : n
        x = xa(i);
        y = ya(i);
        rx = [x, x+0.31, x+0.31, x];
        ry = [y, y , y+0.31, y+0.31];
        fill(rx, ry, ca(i), 'edgealpha', 0);
end