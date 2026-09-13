"""ai_devops paketi: Otonom DevOps motorunun alt bileşenleri.

- gitflow_agent   : Pillar 1 -- ham git diff'i yapısal bir değişiklik
                     analizine (ChangeAnalysis) çevirir.
- task_generator  : Pillar 3 -- ChangeAnalysis'e göre kalıcı, dedup'lı,
                     severity/ID'li görevler üretir (ai-improvement-tasks.txt).
- testing_engine  : Pillar 2 -- test'siz kalan paketler için testify
                     tabanlı (t.Skip ile işaretli, dürüst) test iskeletleri
                     üretir.
"""