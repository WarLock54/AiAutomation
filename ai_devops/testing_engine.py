"""Autonomous Testing Engine (Pillar 2)
=========================================

Değişen Go dosyalarını (bkz. gitflow_agent.ChangeAnalysis) tarar, HİÇ test
dosyası bulunmayan paketlerdeki dışa açık (exported) fonksiyon/metodları
tespit eder ve her paket için testify tabanlı bir test İSKELETİ üretir.

Eskiden bu motor SADECE tek, sabit bir dosya (orchestrator için) üretiyordu
-- hangi paketin gerçekten değiştiğiyle hiç ilgisi yoktu. Artık:
  - Değişen HER paket ayrı ayrı değerlendirilir,
  - Paket zaten en az bir `_test.go` dosyasına sahipse ATLANIR (mevcut
    insan testlerinin üzerine asla yazılmaz),
  - Sadece test'siz kalan paketler için, o paketteki değişen dosyanın
    exported fonksiyonlarına özel bir iskelet üretilir.

ÖNEMLİ (bkz. rapor [O1]): üretilen HER test fonksiyonu `t.Skip(...)` ile
işaretlenir. Bu motor statik bir regex taramasıdır -- bir fonksiyonun
DAVRANIŞINI bilemez, bu yüzden gerçek assertion'lar yazamaz. Amacı, insan
mühendise "bu paket değişti ama hiç test dosyası yok, işte başlaman için
bir iskelet" demektir; CI'da asla "yeşil = doğrulandı" yanılsaması
yaratmaz -- `go test` çıktısında bu fonksiyonlar SKIP olarak ayrıca
raporlanır.
"""

from __future__ import annotations

import os
import re

_EXPORTED_FUNC_RE = re.compile(r"^func\s+(?:\([^)]*\)\s+)?([A-Z]\w*)\s*\(")
_PACKAGE_RE = re.compile(r"^package\s+(\w+)")
_MAX_FUNCS_PER_FILE = 6


def _read_package_name(path: str) -> str | None:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = _PACKAGE_RE.match(line.strip())
                if m:
                    return m.group(1)
    except OSError:
        return None
    return None


def _extract_exported_funcs(path: str) -> list[str]:
    funcs: list[str] = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = _EXPORTED_FUNC_RE.match(line.rstrip())
                if m:
                    funcs.append(m.group(1))
    except OSError:
        pass
    return funcs


def _package_has_any_test_file(directory: str) -> bool:
    try:
        return any(fn.endswith("_test.go") for fn in os.listdir(directory))
    except OSError:
        return False


def _render_test_file(package_name: str, source_base: str, exported_funcs: list[str]) -> str:
    test_funcs = []
    for name in exported_funcs:
        test_funcs.append(f"""// Test{name}_AI_Generated_TODO, "{source_base}.go" icindeki disa acik
// {name} icin AI DevOps Engine (Autonomous Testing Engine) tarafindan
// uretilen bir ISKELETTIR. Bilincli olarak t.Skip ile isaretlenmistir
// (bkz. rapor [O1]): statik analiz {name}'in davranisini bilemez, bu
// yuzden yesil bir test asla "dogrulandi" anlamina gelmez.
func Test{name}_AI_Generated_TODO(t *testing.T) {{
\tt.Skip("AI tarafindan uretilen iskelet: {name} icin gercek table-driven assertion'lar eklenip insan onayindan gecmeden aktif edilmemelidir")

\t// TODO(insan): table-driven test case'ler, gerekiyorsa mock/fake
\t// bagimliliklar ve testify/require ile gercek assertion'lar ekleyin.
\t_ = require.New(t)
}}""")

    # NOT: gofmt, üst düzey (top-level) bildirimler arasında TAM OLARAK bir
    # boş satır bekler -- iki değil. Her fonksiyon bloğu zaten kendi başına
    # tam metindir (baştan/sondan fazladan \n İÇERMEZ); bu yüzden bloklar
    # arasına "\n\n" (bir boş satır) koyuyoruz. Eskiden burada "\n".join
    # kullanılıyordu ve her blok f-string'in kendisinden gelen bir fazladan
    # \n ile birleşince ÇİFT boş satır oluşuyordu -- bu da üretilen dosyanın
    # gofmt'a uymamasına ve CI'ın "Verify formatting" adımında başarısız
    # olmasına yol açıyordu (bu hatayı gerçek CI çalıştırmasında bulduk).
    body = "\n\n".join(test_funcs)
    return f'''package {package_name}

import (
\t"testing"

\t"github.com/stretchr/testify/require"
)

// Bu dosya AI DevOps Engine (Autonomous Testing Engine) tarafindan otonom
// olarak uretilmistir: "{source_base}.go" degisti ama bu pakette HIC test
// dosyasi bulunamadi. Uretilen her fonksiyon KASITLI olarak t.Skip ile
// isaretlidir (bkz. rapor [O1] -- asla sahte-yesil bir test uretilmez).
// Bir muhendis bu dosyayi tamamlayip insan onayindan gecirmeden CI'da
// aktif bir kalite kapisi olarak SAYILMAMALIDIR.

{body}
'''


def generate_test_skeletons(
    changed_go_files: list[str],
    changed_go_test_files: list[str],
) -> list[str]:
    """Test dosyası OLMAYAN paketlerdeki değişen kaynak dosyalar için
    testify tabanlı iskelet test dosyaları üretir.

    changed_go_test_files parametresi şu an sadece belgeleme/log amaçlı
    tutuluyor -- asıl "bu paketin testi var mı" kararı, DEĞİŞEN dosyanın
    kendi dizinindeki TÜM `_test.go` dosyalarına (sadece bu commit'te
    değişenlere değil) bakılarak veriliyor; böylece "paket zaten test
    edilebilir durumda ama bu commit'te test dosyası değişmedi" yanlış
    pozitifini önlüyoruz.

    Döner: oluşturulan yeni dosya yollarının listesi (boş olabilir).
    """
    del changed_go_test_files  # bkz. yukarıdaki not; şu an kullanılmıyor

    non_test_files = [f for f in changed_go_files if not f.endswith("_test.go")]
    created: list[str] = []
    seen_dirs: set[str] = set()

    for src_path in non_test_files:
        directory = os.path.dirname(src_path) or "."
        if directory in seen_dirs:
            continue

        if not os.path.isdir(directory):
            continue  # bu checkout'ta dizin yok (kısmi/monorepo alt-checkout senaryosu)

        if _package_has_any_test_file(directory):
            continue  # paket zaten en az bir teste sahip, insan testlerinin üzerine yazma

        seen_dirs.add(directory)

        package_name = _read_package_name(src_path)
        if not package_name:
            continue

        exported = _extract_exported_funcs(src_path)
        if not exported:
            continue
        exported = exported[:_MAX_FUNCS_PER_FILE]

        base = os.path.splitext(os.path.basename(src_path))[0]
        out_path = os.path.join(directory, f"{base}_ai_test.go")
        if os.path.exists(out_path):
            continue

        content = _render_test_file(package_name, base, exported)
        with open(out_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)
        created.append(out_path)

    return created