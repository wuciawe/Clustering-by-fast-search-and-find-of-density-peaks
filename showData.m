function showData(filename)
data = load(filename);
x=data(:,1);
y=data(:,2);
scatter(x,y,'black');
