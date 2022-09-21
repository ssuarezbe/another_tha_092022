# 

```
PYTHONPATH=.. python solution.py
```

```
$ sqlite3 /tmp/part2_qventus.db
sqlite> .timer on
sqlite> select * from procedure_text_search where procedure_name match 'hospi*';
...
Run Time: real 0.002 user 0.000577 sys 0.000770
sqlite>
```
