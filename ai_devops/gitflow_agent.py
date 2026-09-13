"""GitFlow & Commit Agent (Pillar 1)
=====================================

Bu modül, ham bir git diff'i YAPISAL bir değişiklik analizine çevirir:
hangi dosyalar eklendi/değiştirildi/silindi/yeniden adlandırıldı, hangi
servisleri etkiliyor, test/migration/Dockerfile/bağımlılık/workflow
değişikliği var mı gibi.

Eskiden bu analiz sadece "diff metninde 'order-service' string'i geçiyor
mu" gibi kaba bir substring aramasıydı (bkz. rapor [O2]) -- bu, rename'leri
silinen dosyaları, ya da path'in bir yorum satırında mı yoksa gerçek bir
dosya yolunda mı geçtiğini ayırt edemiyordu. Artık `git diff --name-status`
çıktısını satır satır parse edip yapılandırılmış bir ChangeAnalysis nesnesi
üretiyoruz; task_generator ve testing_engine kararlarını BU nesneye göre
verir, ham metne göre değil.
"""

from __future__ import annotations

import dataclasses
import subprocess

# Bilinen servis dizinleri -> insan-okur isim. Yeni bir servis eklendiğinde
# sadece burayı güncellemek yeterlidir.
KNOWN_SERVICES = {
    "order-project/order-engine/services/order-service": "order-service",
    "order-project/order-engine/services/inventory-service": "inventory-service",
    "order-project/order-engine/services/payment-service": "payment-service",
    "order-project/order-engine/services/notification-consumer": "notification-consumer",
    "order-project/order-engine/pkg": "shared-pkg",
}


@dataclasses.dataclass
class ChangedFile:
    status: str  # 'A' (added), 'M' (modified), 'D' (deleted), 'R' (renamed) vb.
    path: str
    old_path: str | None = None  # sadece rename'lerde dolu


@dataclasses.dataclass
class ChangeAnalysis:
    files: list[ChangedFile]
    affected_services: set[str]
    changed_go_files: list[str]
    changed_go_test_files: list[str]
    changed_migration_files: list[str]
    changed_dockerfiles: list[str]
    changed_workflow_files: list[str]
    go_mod_changed: bool

    @property
    def has_code_without_matching_tests(self) -> bool:
        """Değişen (non-test) bir .go dosyası var ama HİÇ .go test dosyası
        değişmemiş mi? (Paket bazlı, daha kesin bir kontrol testing_engine
        tarafından ayrıca yapılır; bu sadece hızlı bir görev-üretim
        sinyalidir.)"""
        non_test = [f for f in self.changed_go_files if not f.endswith("_test.go")]
        return bool(non_test) and not self.changed_go_test_files


def _run_git(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], capture_output=True, text=True)


def get_name_status(base_ref: str) -> list[ChangedFile]:
    """`git diff --name-status` çıktısını parse eder.

    base_ref, ai_devops_engine.resolve_base_ref()'in döndürdüğü değerdir:
    ya "HEAD~1" ya da git'in her depoda var olan sabit boş-ağaç SHA'sı
    (ilk commit / yetersiz shallow history senaryosu). Her iki durumda da
    aynı `git diff --name-status <base_ref> HEAD` komutu güvenle çalışır;
    ayrı bir "ilk commit" özel durumuna gerek yoktur.

    Komut herhangi bir nedenle başarısız olursa (örn. bozuk repo) boş
    liste döner -- bu fonksiyon KENDİSİ hata fırlatmaz; çağıran taraf
    (ai_devops_engine.py) zaten git durumunu daha önce doğrulamış olmalı.
    """
    result = _run_git(["diff", "--name-status", base_ref, "HEAD"])
    if result.returncode != 0:
        return []

    files: list[ChangedFile] = []
    for line in result.stdout.splitlines():
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = line.split("\t")
        status = parts[0]
        if status.startswith(("R", "C")) and len(parts) == 3:
            # Rename/Copy: "R100\told_path\tnew_path"
            files.append(ChangedFile(status=status[0], path=parts[2], old_path=parts[1]))
        elif len(parts) >= 2:
            files.append(ChangedFile(status=status[0], path=parts[1]))
    return files


def build_change_analysis(base_ref: str) -> ChangeAnalysis:
    """Verilen temel referansa göre tam bir ChangeAnalysis üretir."""
    files = get_name_status(base_ref)

    affected_services: set[str] = set()
    changed_go_files: list[str] = []
    changed_go_test_files: list[str] = []
    changed_migration_files: list[str] = []
    changed_dockerfiles: list[str] = []
    changed_workflow_files: list[str] = []
    go_mod_changed = False

    for f in files:
        if f.status == "D":
            # Silinen bir dosya için servis/test analizi anlamsız (artık
            # kod tabanında yok); yine de `files` listesinde tutulur ki
            # görünürlük kaybolmasın.
            continue

        for prefix, service_name in KNOWN_SERVICES.items():
            if f.path.startswith(prefix):
                affected_services.add(service_name)
                break

        if f.path.endswith(".go"):
            changed_go_files.append(f.path)
            if f.path.endswith("_test.go"):
                changed_go_test_files.append(f.path)
        elif f.path.endswith(".sql") and "/migrations/" in f.path:
            changed_migration_files.append(f.path)
        elif f.path.endswith("Dockerfile"):
            changed_dockerfiles.append(f.path)
        elif f.path.startswith(".github/workflows/"):
            changed_workflow_files.append(f.path)
        elif f.path.endswith("go.mod") or f.path.endswith("go.sum"):
            go_mod_changed = True

    return ChangeAnalysis(
        files=files,
        affected_services=affected_services,
        changed_go_files=changed_go_files,
        changed_go_test_files=changed_go_test_files,
        changed_migration_files=changed_migration_files,
        changed_dockerfiles=changed_dockerfiles,
        changed_workflow_files=changed_workflow_files,
        go_mod_changed=go_mod_changed,
    )