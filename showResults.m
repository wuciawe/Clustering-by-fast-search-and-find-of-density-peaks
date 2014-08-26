function showResults(fileName)
s = 0.5;
subplot(2,2,1);
showData(fileName);
title(fileName);
subplot(2,2,2);
showIdeal(fileName, s);
title('Ideal Results');
subplot(2,2,3);
printRhoDelta();
title('rho-delta');
xlabel('rho');
ylabel('delta');
subplot(2,2,4);
showResult(s);
title('Clustering Results');



function printRhoDelta()
data = load('rhodelta');
grid on;
hold on;
x=data(:,3);
y=data(:,4);
scatter(x,y);

function showData(filename)
data = load(filename);
x=data(:,1);
y=data(:,2);
scatter(x,y,'.','b');

function showIdeal(filename, s)
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
        rx = [x, x+s, x+s, x];
        ry = [y, y , y+s, y+s];
        fill(rx, ry, ca(i), 'edgealpha', 0);
end

function showResult(s)
A = importdata('result', '\n');
n = size(A, 1);
hold on;
caxis([1,n]);
for i = 1 : n
    pts = regexp(A{i,1}, '[, ]', 'split');
    len = numel(pts) / 2;
    rgb = [rand, rand, rand];
    for j = 1 : len
        x = str2num(pts{1, 2*j-1});
        y = str2num(pts{1, 2*j});
        rx = [x, x+s, x+s, x];
        ry = [y, y , y+s, y+s];
        fill(rx, ry, i, 'edgealpha', 0);        
    end
end