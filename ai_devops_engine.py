"""AI DevOps Engine — Orkestratör
===================================

Bu script her push'ta çalışır ve üç otonom bileşeni sırayla yürütür:

  1. GitFlow & Commit Agent    (ai_devops.gitflow_agent)
     Ham git diff'ini yapısal bir değişiklik analizine (ChangeAnalysis)
     çevirir: hangi dosyalar/servisler/testler/migration'lar değişti.

  2. Task Generator            (ai_devops.task_generator)
     ChangeAnalysis'e göre `ai-improvement-tasks.txt`'yi GÜNCELLER (üzerine
     yazmaz) -- her görev benzersiz bir ID, severity, created/last_seen
     zaman damgası taşır; aynı bulgu tekrar tespit edildiğinde yeni bir
     satır eklenmez.

  3. Autonomous Testing Engine  (ai_devops.testing_engine)
     Test'siz kalan paketler için testify tabanlı, KASITLI t.Skip ile
     işaretli test iskeletleri üretir (bkz. rapor [O1] -- asla sahte-yeşil
     bir test üretilmez).

Hata yönetimi: herhangi bir adım sessizce başarısız olup "her şey stabil"
gibi yanlış bir rapor üretmemelidir (bkz. rapor [K1]). Bu yüzden git
geçmişinin yetersiz olduğu (ilk commit, shallow clone, vb.) durumlar
açıkça ele alınır ve gerçek bir hata durumunda pipeline exit(1) ile
başarısız olur.
"""

from __future__ import annotations

import subprocess
import sys

from ai_devops import gitflow_agent, task_generator, testing_engine


class GitAnalysisError(RuntimeError):
    """git diff analizi güvenilir şekilde yapılamadığında fırlatılır."""


# Git'in İÇERİK TABANLI ADRESLEME sayesinde HER depoda (açıkça yazılmasa
# bile) zaten var olan, sabit "boş ağaç" nesnesinin SHA'sı. İlk commit'i
# (ya da yetersiz shallow history'yi) "sanki HEAD~1'i boş bir ağaçmış gibi"
# ele almak için kullanılır -- bkz. resolve_base_ref.
EMPTY_TREE_SHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def _run_git(args: list[str]) -> subprocess.CompletedProcess:
    # encoding/errors AÇIKÇA belirtiliyor: subprocess.run(text=True) tek
    # başına işletim sisteminin/bölgenin varsayılan kodlamasını kullanır
    # (örn. Türkçe Windows'ta bu genelde UTF-8 DEĞİL, cp1254'tür). git
    # çıktısı (özellikle Türkçe karakter içeren dosya/commit içerikleri
    # dolayısıyla) UTF-8 olduğu için, cp1254 ile decode etmeye çalışmak
    # `UnicodeDecodeError` ile arka plandaki okuma thread'inin çökmesine
    # ve sonuçta `result.stdout`'un SESSİZCE None kalmasına yol açıyordu
    # (bu da aşağı akışta bir AttributeError'a neden oluyordu). encoding
    # ve errors burada sabitlenerek bu platforma özgü kırılganlık ortadan
    # kaldırılıyor.
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _has_previous_commit() -> bool:
    """HEAD~1 gerçekten var mı diye bakar (ilk commit'te yoktur)."""
    result = _run_git(["rev-parse", "--verify", "-q", "HEAD~1"])
    return result.returncode == 0


def _has_any_commit() -> bool:
    """HEAD'in kendisi çözümlenebiliyor mu diye bakar.

    Bu False dönerse, depoda HENÜZ HİÇ commit yoktur (sadece `git init`
    yapılmış olabilir) -- bu durumda HEAD~1 de HEAD'in kendisi de yoktur,
    ve `git diff <herhangi bir ref> HEAD` her zaman "unknown revision"
    hatasıyla başarısız olur. Bu, gerçek bir analiz hatası DEĞİLDİR;
    sadece "henüz analiz edilecek bir commit yok" anlamına gelir (bkz.
    main() -- bu durum ayrı ve açık şekilde ele alınır, ham bir git
    hatası olarak kullanıcıya sızdırılmaz).
    """
    result = _run_git(["rev-parse", "--verify", "-q", "HEAD"])
    return result.returncode == 0


def _is_shallow_repo() -> bool:
    result = _run_git(["rev-parse", "--is-shallow-repository"])
    return result.returncode == 0 and result.stdout.strip() == "true"


def resolve_base_ref() -> tuple[str | None, bool]:
    """Analiz için kullanılacak temel referansı belirler.

    Döner: (base_ref, used_fallback)
      - base_ref="HEAD~1": normal iki-commit diff'i kullanılabilir.
      - base_ref=EMPTY_TREE_SHA: HEAD~1 yok (ilk commit ya da yetersiz
        shallow history); git'in HER repoda var olan sabit boş-ağaç
        nesnesiyle (bkz. EMPTY_TREE_SHA) karşılaştırılır.
    used_fallback, ikinci durumda True olur (sadece log/bilgilendirme
    amaçlı).

    Eskiden burada doğrudan `git diff HEAD~1` çalıştırılıyordu; bu, ilk
    commit'te veya `fetch-depth: 1` gibi shallow checkout'larda HEAD~1
    bulunamadığı için sessizce boş/hatalı çıktı üretiyor ve pipeline bunu
    "değişiklik yok, her şey stabil" olarak yanlış yorumluyordu (bkz.
    rapor [K1]).

    NOT: Daha önce bu durumda `git diff --root HEAD` deneniyordu, ancak
    `--root` bayrağı `git log`/`git diff-tree` içindir; düz `git diff
    <tek-commit>` commit'i WORKING DIRECTORY ile karşılaştırır, boş
    ağaçla değil -- bu yüzden ilk commit'te yanlışlıkla "0 dosya değişti"
    sonucu üretiyordu. `EMPTY_TREE_SHA`, her git deposunda (içerik
    tabanlı adresleme sayesinde) İÇLİĞİNDEN dolayı zaten var olan sabit
    bir boş ağaç nesnesidir; bu yüzden `git diff EMPTY_TREE_SHA HEAD`
    her zaman güvenle çalışır ve "ilk commit" özel durumunu ayrı bir
    kod yoluna gerek kalmadan doğal olarak ele alır.
    """
    if _has_previous_commit():
        return "HEAD~1", False
    return EMPTY_TREE_SHA, True


def analyze_git_diff(base_ref: str) -> str:
    """Verilen temel referansa göre ham diff METNİNİ döner.

    base_ref her zaman geçerli bir referanstır (HEAD~1 ya da
    EMPTY_TREE_SHA); bu yüzden tek bir kod yolu yeterlidir -- ayrı bir
    "ilk commit" özel durumuna gerek yoktur (bkz. resolve_base_ref).

    NOT: Bu metin artık karar vermek için KULLANILMIYOR -- kararlar
    gitflow_agent.build_change_analysis()'in ürettiği yapısal
    ChangeAnalysis üzerinden veriliyor (bkz. rapor [O2]). Bu fonksiyon
    sadece log/debug amaçlı ve geriye dönük uyumluluk için tutuluyor.
    """
    result = _run_git(["diff", base_ref, "HEAD"])
    if result.returncode != 0:
        raise GitAnalysisError(
            f"'git diff {base_ref} HEAD' başarısız oldu (kod={result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout


def main() -> None:
    print("[*] Git commit ve kod değişiklikleri taranıyor...")

    if not _has_any_commit():
        # Depoda henüz hiç commit yok (örn. sadece `git init` yapılmış,
        # ilk `git commit` atılmamış). Bu bir HATA değildir -- gerçek CI
        # akışında (her push en az bir commit içerir) bu durum zaten
        # oluşmaz; sadece yerel bir deneme senaryosudur. Ham bir git
        # hatasıyla (örn. "unknown revision") crash etmek yerine
        # AÇIKÇA bilgilendirip başarıyla çıkıyoruz.
        print(
            "[i] Bu depoda henüz hiç commit yok; analiz edilecek bir "
            "değişiklik bulunmuyor. En az bir 'git commit' attıktan sonra "
            "tekrar çalıştırın."
        )
        return

    base_ref, used_fallback = resolve_base_ref()
    if used_fallback:
        print(
            "[!] HEAD~1 bulunamadı (ilk commit veya shallow history); "
            "ilk commit'in kök (root) diff'i kullanılacak."
        )
        if _is_shallow_repo():
            print("[i] Depo shallow (sığ) clone edilmiş; sadece görünen commit analiz edildi.")

    try:
        diff_text = analyze_git_diff(base_ref)
    except GitAnalysisError as exc:
        print(f"[HATA] Git analizi başarısız: {exc}", file=sys.stderr)
        sys.exit(1)

    if not diff_text.strip() and not used_fallback:
        # Gerçek bir "değişiklik yok" durumu (örn. sadece dosya izinleri
        # değişti); pipeline'ı FAIL etmeye gerek yok ama bunu açıkça
        # logluyoruz -- eskiden bu durum sessizce "stabil" sayılırdı.
        print("[i] HEAD~1 ile HEAD arasında içerik farkı bulunamadı.")

    # --- 1. GitFlow & Commit Agent ---
    print("[*] Değişiklikler yapısal olarak analiz ediliyor (GitFlow & Commit Agent)...")
    analysis = gitflow_agent.build_change_analysis(base_ref)
    print(
        f"[i] {len(analysis.files)} dosya değişti | "
        f"etkilenen servisler: {sorted(analysis.affected_services) or ['—']} | "
        f".go dosyası: {len(analysis.changed_go_files)} "
        f"(bunlardan test dosyası: {len(analysis.changed_go_test_files)}) | "
        f"migration: {len(analysis.changed_migration_files)} | "
        f"Dockerfile: {len(analysis.changed_dockerfiles)} | "
        f"go.mod/go.sum değişti: {analysis.go_mod_changed}"
    )

    # --- 2. Task Generator ---
    print(f"[*] Görevler güncelleniyor ({task_generator.TASK_FILE})...")
    tasks = task_generator.generate_tasks(analysis)
    print(f"[+] Toplam {len(tasks)} görev güncel (yeni + daha önce görülmüş, dedup edilmiş).")

    # --- 3. Autonomous Testing Engine ---
    print("[*] Test'siz kalan paketler taranıyor (Autonomous Testing Engine)...")
    created = testing_engine.generate_test_skeletons(
        analysis.changed_go_files, analysis.changed_go_test_files
    )
    if created:
        for path in created:
            print(f"[+] Yeni test iskeleti oluşturuldu (t.Skip ile işaretli): {path}")
    else:
        print(
            "[i] Test iskeleti gerektiren bir paket bulunamadı "
            "(değişen paketler zaten test'e sahip ya da hiç .go değişikliği yok)."
        )


if __name__ == "__main__":
    main()