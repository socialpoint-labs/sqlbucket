select {% for numb in f.nrange2(1, 5) %}{{ numb }}{%if not loop.last %}, {% endif %}{% endfor %};
