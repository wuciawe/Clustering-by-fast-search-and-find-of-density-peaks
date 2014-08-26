function showResult()
A = importdata('result', '\n');
[n, tmp] = size(A)
hold on;
%colormap(jet(16));
caxis([1,n]);
for i = 1 : n
    pts = regexp(A{i,1}, '[, ]', 'split');
    len = numel(pts) / 2;
    rgb = [rand, rand, rand];
    for j = 1 : len
        x = str2num(pts{1, 2*j-1});
        y = str2num(pts{1, 2*j});
        %c = str2num(pts{1, 3*j});
        rx = [x, x+0.31, x+0.31, x];
        ry = [y, y , y+0.31, y+0.31];
        fill(rx, ry, i, 'edgealpha', 0);
        %plot(x,y,'.','Color',rgb);
        
        %th = 0:pi/50:2*pi;
        %xunit = 2 * cos(th) + x;
        %yunit = 2 * sin(th) + y;
        %h = plot(xunit, yunit, 'Color', rgb);
        
    end
end