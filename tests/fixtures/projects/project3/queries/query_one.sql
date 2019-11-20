select {% for numb in f.nrange(1, 4) %}{{ numb }}{%if not loop.last %}, {% endif %}{% endfor %};
