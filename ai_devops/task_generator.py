"""Task Generator (Pillar 3)
==============================

ChangeAnalysis'e (bkz. gitflow_agent) göre insan-okur iyileştirme görevleri
üretir ve bunları `ai-improvement-tasks.txt` dosyasına kalıcı olarak yazar.

Eskiden bu dosya HER pipeline çalıştırmasında SIFIRDAN yazılıyordu (bkz.
rapor önerisi 4.7: "ayni bulguyu her pipeline'da tekrar yazma"). Bu, iki
sorun yaratıyordu:
  1. Bir mühendis bir görevi "yaptım" diye kapatsa bile, bir sonraki push
     aynı bulguyu (aynı kelimelerle) yeniden üretip dosyayı sıfırlıyordu --
     tarihçe/ilerleme kaybediliyordu.
  2. Görevlerin benzersiz bir kimliği, oluşturulma zamanı, ya da "en son ne
     zaman tekrar tespit edildiği" bilgisi yoktu.

Artık her görev, açıklamasından türetilen DETERMİNİSTİK bir ID taşır: aynı
bulgu tekrar tespit edildiğinde yeni bir satır EKLENMEZ, sadece
`last_seen` alanı güncellenir. `created` alanı bulgunun İLK görüldüğü anı
korur. Böylece dosya, zaman içinde biriken (ve insan tarafından silinerek
kapatılabilen) bir görev listesi haline gelir.
"""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import os
import re

from ai_devops.gitflow_agent import ChangeAnalysis

TASK_FILE = "ai-improvement-tasks.txt"

_SEVERITY_ORDER = {"KRITIK": 0, "ONEMLI": 1, "BILGI": 2}

_LINE_RE = re.compile(
    r"^\[(?P<id>[0-9a-f]{10})\]\[(?P<severity>[A-ZİÜŞÖÇĞ]+)\]"
    r"\[created=(?P<created>[^\]]+)\]\[last_seen=(?P<last_seen>[^\]]+)\]\s+(?P<desc>.+)$"
)


@dataclasses.dataclass
class Task:
    id: str
    severity: str  # KRITIK, ONEMLI, BILGI
    description: str
    created_at: str  # ISO 8601, bulgunun İLK tespit edildiği an
    last_seen_at: str  # ISO 8601, en son hangi çalıştırmada tekrar görüldüğü


def _task_id(description: str) -> str:
    """Aynı bulgunun her pipeline çalıştırmasında farklı bir ID almaması
    için açıklamanın kendisinden deterministik bir kısa hash üretilir."""
    return hashlib.sha1(description.encode("utf-8")).hexdigest()[:10]


def _build_candidate_tasks(analysis: ChangeAnalysis) -> list[tuple[str, str]]:
    """ChangeAnalysis'e göre (severity, açıklama) çiftleri üretir."""
    candidates: list[tuple[str, str]] = []

    if "order-service" in analysis.affected_services:
        candidates.append((
            "ONEMLI",
            "Order-service değişikliği: Saga orchestrator adımlarındaki "
            "compensation (telafi) senaryoları için testify testleri "
            "güncel mi kontrol edilmeli.",
        ))
    if "payment-service" in analysis.affected_services:
        candidates.append((
            "KRITIK",
            "Payment-service değişikliği: idempotency anahtarı için "
            "eşzamanlı (concurrent) istek/yük testi çalıştırılmalı.",
        ))
    if "inventory-service" in analysis.affected_services:
        candidates.append((
            "ONEMLI",
            "Inventory-service değişikliği: stok commit/release durum "
            "geçişleri için eşzamanlılık (concurrency) testi kontrol edilmeli.",
        ))
    if "notification-consumer" in analysis.affected_services:
        candidates.append((
            "ONEMLI",
            "Notification-consumer değişikliği: Redis tabanlı dedup "
            "mantığı restart senaryosuyla yeniden doğrulanmalı.",
        ))
    if "shared-pkg" in analysis.affected_services:
        candidates.append((
            "KRITIK",
            "Paylaşılan pkg/ paketi değişti: bu paketi kullanan TÜM "
            "servislerde regresyon riski var, entegrasyon testleri "
            "çalıştırılmalı.",
        ))

    if analysis.has_code_without_matching_tests:
        non_test = [f for f in analysis.changed_go_files if not f.endswith("_test.go")]
        shown = ", ".join(non_test[:5])
        suffix = ", ..." if len(non_test) > 5 else ""
        candidates.append((
            "KRITIK",
            f"Değişen Go dosyaları ({shown}{suffix}) için bu commit'te hiç "
            "test dosyası değişmemiş -- test coverage'ın gözden kaçırılmış "
            "olma riski var.",
        ))

    if analysis.changed_migration_files:
        candidates.append((
            "ONEMLI",
            "SQL migration değişikliği tespit edildi: README'deki migration "
            "listesinin ve uygulama sırasının güncel olduğu doğrulanmalı.",
        ))

    if analysis.changed_dockerfiles:
        candidates.append((
            "ONEMLI",
            "Dockerfile değişikliği tespit edildi: checksum doğrulama, "
            "non-root kullanıcı ve base image sürümünün korunduğu kontrol "
            "edilmeli (bkz. rapor [O5]).",
        ))

    if analysis.go_mod_changed:
        candidates.append((
            "KRITIK",
            "go.mod/go.sum değişti: govulncheck taraması sonuçları (CI'da "
            "otomatik çalışıyor) insan tarafından teyit edilmeli.",
        ))

    if analysis.changed_workflow_files:
        candidates.append((
            "KRITIK",
            "CI/CD workflow dosyası değişti: bu değişikliğin branch "
            "protection/required-checks kurallarını bozmadığı doğrulanmalı.",
        ))

    if not candidates:
        candidates.append((
            "BILGI",
            "Bu değişiklik kümesi bilinen servis/test/migration/Dockerfile "
            "kalıplarıyla eşleşmedi; genel bir kod incelemesi yeterli olabilir.",
        ))

    return candidates


def _parse_existing_tasks(path: str) -> dict[str, Task]:
    """Daha önce yazılmış görev dosyasını okuyup mevcut görevleri geri
    yükler. Dosya yoksa ya da format tanınmıyorsa boş sözlük döner --
    bu fonksiyon ASLA crash etmez (bozuk/elle düzenlenmiş bir dosya,
    pipeline'ı durdurmamalıdır)."""
    tasks: dict[str, Task] = {}
    if not os.path.exists(path):
        return tasks

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                m = _LINE_RE.match(line.strip())
                if not m:
                    continue
                tasks[m.group("id")] = Task(
                    id=m.group("id"),
                    severity=m.group("severity"),
                    description=m.group("desc"),
                    created_at=m.group("created"),
                    last_seen_at=m.group("last_seen"),
                )
    except OSError:
        return {}
    return tasks


def generate_tasks(analysis: ChangeAnalysis, task_file: str = TASK_FILE) -> list[Task]:
    """ChangeAnalysis'e göre görev üretir, VAR OLAN görev dosyasıyla
    BİRLEŞTİRİR (overwrite değil merge) ve dosyayı yeniden yazar.

    Döner: severity'ye göre sıralanmış tüm görevlerin (yeni + daha önce
    görülmüş) listesi.
    """
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    existing = _parse_existing_tasks(task_file)

    for severity, description in _build_candidate_tasks(analysis):
        task_id = _task_id(description)
        if task_id in existing:
            existing[task_id].last_seen_at = now
        else:
            existing[task_id] = Task(
                id=task_id,
                severity=severity,
                description=description,
                created_at=now,
                last_seen_at=now,
            )

    ordered = sorted(
        existing.values(),
        key=lambda t: (_SEVERITY_ORDER.get(t.severity, 99), t.created_at),
    )

    with open(task_file, "w", encoding="utf-8", newline="\n") as f:
        f.write("=== AI DEVOPS & KOD IYILESTIRME GOREVLERI ===\n")
        f.write(f"# Son guncelleme (UTC): {now}\n")
        f.write(f"# Toplam gorev: {len(ordered)}\n")
        f.write(
            "# Format: [id][severity][created=...][last_seen=...] aciklama\n"
            "# Bir gorev tamamlandiginda ilgili satiri silin; bir sonraki "
            "calistirmada bulgu hala gecerliyse yeniden eklenecektir.\n\n"
        )
        for t in ordered:
            f.write(
                f"[{t.id}][{t.severity}][created={t.created_at}]"
                f"[last_seen={t.last_seen_at}] {t.description}\n"
            )

    return ordered