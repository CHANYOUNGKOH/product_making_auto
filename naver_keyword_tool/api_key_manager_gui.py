"""
api_key_manager_gui.py

네이버 API 키 관리 GUI.
.env 파일의 API 키를 시각적으로 확인/추가/삭제할 수 있음.
  - 검색광고 API (키워드도구 / 연관키워드)
  - 데이터랩 오픈API (검색어트렌드 / 쇼핑인사이트)
  - 셀러센터 로그인 정보
"""
from __future__ import annotations

import os
import re
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from collections import OrderedDict

# ── .env 경로 ──────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_THIS_DIR)
_ENV_PATH = os.path.join(_PROJECT_ROOT, ".env")

# ── 색상 ──────────────────────────────────────────────────
COLOR_BG = "#F5F5F5"
COLOR_AD = "#1565C0"      # 검색광고
COLOR_DATALAB = "#2E7D32"  # 데이터랩
COLOR_SELLER = "#6A1B9A"   # 셀러센터
COLOR_OK = "#4CAF50"
COLOR_WARN = "#FF9800"
COLOR_ERR = "#F44336"


def _read_env() -> OrderedDict[str, str]:
    """Read .env file as ordered dict preserving comments."""
    data = OrderedDict()
    if not os.path.exists(_ENV_PATH):
        return data
    with open(_ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n\r")
            if "=" in line and not line.lstrip().startswith("#"):
                key, _, val = line.partition("=")
                data[key.strip()] = val.strip()
    return data


def _read_env_raw() -> str:
    if not os.path.exists(_ENV_PATH):
        return ""
    with open(_ENV_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _write_env_raw(content: str):
    with open(_ENV_PATH, "w", encoding="utf-8") as f:
        f.write(content)


def _set_env_value(key: str, value: str):
    """Set or add a key=value in .env file."""
    raw = _read_env_raw()
    pattern = re.compile(rf"^{re.escape(key)}\s*=.*$", re.MULTILINE)
    if pattern.search(raw):
        raw = pattern.sub(f"{key}={value}", raw)
    else:
        if not raw.endswith("\n"):
            raw += "\n"
        raw += f"{key}={value}\n"
    _write_env_raw(raw)


def _remove_env_keys(keys: list[str]):
    """Remove keys from .env file."""
    raw = _read_env_raw()
    for key in keys:
        pattern = re.compile(rf"^{re.escape(key)}\s*=.*\n?", re.MULTILINE)
        raw = pattern.sub("", raw)
    _write_env_raw(raw)


class APIKeyManagerGUI:
    def __init__(self, master=None):
        if master is None:
            self.root = tk.Tk()
            self._standalone = True
        else:
            self.root = tk.Toplevel(master)
            self._standalone = False

        self.root.title("네이버 API 키 관리")
        self.root.geometry("720x700")
        self.root.configure(bg=COLOR_BG)
        self.root.resizable(True, True)

        self._build_ui()
        self._refresh()

    def run(self):
        if self._standalone:
            self.root.mainloop()

    # ── UI 구성 ────────────────────────────────────────────
    def _build_ui(self):
        # 상단 요약
        self.summary_frame = tk.Frame(self.root, bg=COLOR_BG)
        self.summary_frame.pack(fill="x", padx=10, pady=(10, 5))

        self.lbl_summary = tk.Label(
            self.summary_frame, text="", bg=COLOR_BG,
            font=("맑은 고딕", 11, "bold"), anchor="w"
        )
        self.lbl_summary.pack(fill="x")

        # 노트북 (탭)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=5)

        # 탭 1: 검색광고 API
        self.tab_ad = tk.Frame(self.notebook, bg=COLOR_BG)
        self.notebook.add(self.tab_ad, text="  검색광고 API  ")

        # 탭 2: 데이터랩 오픈API
        self.tab_datalab = tk.Frame(self.notebook, bg=COLOR_BG)
        self.notebook.add(self.tab_datalab, text="  데이터랩 오픈API  ")

        # 탭 3: 셀러센터
        self.tab_seller = tk.Frame(self.notebook, bg=COLOR_BG)
        self.notebook.add(self.tab_seller, text="  셀러센터  ")

        # 하단 버튼
        btn_frame = tk.Frame(self.root, bg=COLOR_BG)
        btn_frame.pack(fill="x", padx=10, pady=10)

        tk.Button(
            btn_frame, text="새로고침", command=self._refresh,
            bg="#607D8B", fg="white", font=("맑은 고딕", 10), width=10
        ).pack(side="right", padx=5)

        tk.Button(
            btn_frame, text="API 연결 테스트", command=self._test_keys,
            bg="#1976D2", fg="white", font=("맑은 고딕", 10), width=14
        ).pack(side="right", padx=5)

        self.lbl_status = tk.Label(
            btn_frame, text="", bg=COLOR_BG, font=("맑은 고딕", 9), anchor="w"
        )
        self.lbl_status.pack(side="left", padx=5)

    # ── 검색광고 탭 ────────────────────────────────────────
    def _build_ad_tab(self, keys: OrderedDict):
        for w in self.tab_ad.winfo_children():
            w.destroy()

        desc = tk.Label(
            self.tab_ad, bg=COLOR_BG, anchor="w", justify="left",
            font=("맑은 고딕", 9), fg="#666",
            text="검색광고 API는 계정(라이선스)별로 발급됩니다.\n"
                 "추가 병렬 처리가 필요하면 별도 검색광고 계정을 등록 후 키를 추가하세요."
        )
        desc.pack(fill="x", padx=10, pady=(10, 5))

        # 기존 키 표시
        ad_keys = self._extract_ad_keys(keys)
        self.ad_entries = []

        canvas = tk.Canvas(self.tab_ad, bg=COLOR_BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.tab_ad, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas, bg=COLOR_BG)
        scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=5)
        scrollbar.pack(side="right", fill="y", pady=5)

        for idx, (cid, akey, skey) in enumerate(ad_keys):
            num = idx + 1
            frame = tk.LabelFrame(
                scroll_frame, text=f" 키 #{num} ",
                font=("맑은 고딕", 10, "bold"), bg=COLOR_BG,
                fg=COLOR_AD, bd=1, relief="groove"
            )
            frame.pack(fill="x", padx=5, pady=4)

            entries = {}
            for label, val, env_key in [
                ("Customer ID", cid, f"NAVER_AD_CUSTOMER_ID{'_'+str(num) if num > 1 else ''}"),
                ("API Key", akey, f"NAVER_AD_API_KEY{'_'+str(num) if num > 1 else ''}"),
                ("Secret Key", skey, f"NAVER_AD_SECRET_KEY{'_'+str(num) if num > 1 else ''}"),
            ]:
                row = tk.Frame(frame, bg=COLOR_BG)
                row.pack(fill="x", padx=10, pady=2)
                tk.Label(row, text=label, bg=COLOR_BG, font=("맑은 고딕", 9), width=12, anchor="w").pack(side="left")
                ent = tk.Entry(row, font=("Consolas", 9), width=55)
                ent.insert(0, val)
                ent.pack(side="left", padx=5)
                entries[env_key] = ent

            btn_row = tk.Frame(frame, bg=COLOR_BG)
            btn_row.pack(fill="x", padx=10, pady=(2, 5))
            tk.Button(
                btn_row, text="저장", bg=COLOR_AD, fg="white",
                font=("맑은 고딕", 9), width=6,
                command=lambda ents=entries: self._save_keys(ents)
            ).pack(side="left", padx=2)
            if num > 1:
                tk.Button(
                    btn_row, text="삭제", bg=COLOR_ERR, fg="white",
                    font=("맑은 고딕", 9), width=6,
                    command=lambda ents=entries: self._delete_keys(ents)
                ).pack(side="left", padx=2)

            self.ad_entries.append(entries)

        # 추가 버튼
        add_frame = tk.Frame(scroll_frame, bg=COLOR_BG)
        add_frame.pack(fill="x", padx=5, pady=10)
        tk.Button(
            add_frame, text="+ 검색광고 키 추가", bg=COLOR_AD, fg="white",
            font=("맑은 고딕", 10, "bold"), width=20,
            command=lambda: self._add_ad_key(len(ad_keys) + 1)
        ).pack()

    # ── 데이터랩 탭 ────────────────────────────────────────
    def _build_datalab_tab(self, keys: OrderedDict):
        for w in self.tab_datalab.winfo_children():
            w.destroy()

        desc = tk.Label(
            self.tab_datalab, bg=COLOR_BG, anchor="w", justify="left",
            font=("맑은 고딕", 9), fg="#666",
            text="네이버 개발자센터에서 어플리케이션을 추가 등록하면 키가 늘어납니다.\n"
                 "키 1개당 일일 1,000콜. 키를 늘리면 쿼터와 병렬 처리 성능이 함께 증가합니다."
        )
        desc.pack(fill="x", padx=10, pady=(10, 5))

        dl_keys = self._extract_datalab_keys(keys)
        self.dl_entries = []

        canvas = tk.Canvas(self.tab_datalab, bg=COLOR_BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.tab_datalab, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas, bg=COLOR_BG)
        scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=5)
        scrollbar.pack(side="right", fill="y", pady=5)

        for idx, (cid, csec) in enumerate(dl_keys):
            num = idx + 1
            frame = tk.LabelFrame(
                scroll_frame, text=f" 키 #{num} ",
                font=("맑은 고딕", 10, "bold"), bg=COLOR_BG,
                fg=COLOR_DATALAB, bd=1, relief="groove"
            )
            frame.pack(fill="x", padx=5, pady=4)

            entries = {}
            suffix = "" if num == 1 else f"_{num}"
            for label, val, env_key in [
                ("Client ID", cid, f"NAVER_DATALAB_CLIENT_ID{suffix}"),
                ("Client Secret", csec, f"NAVER_DATALAB_CLIENT_SECRET{suffix}"),
            ]:
                row = tk.Frame(frame, bg=COLOR_BG)
                row.pack(fill="x", padx=10, pady=2)
                tk.Label(row, text=label, bg=COLOR_BG, font=("맑은 고딕", 9), width=12, anchor="w").pack(side="left")
                ent = tk.Entry(row, font=("Consolas", 9), width=55)
                ent.insert(0, val)
                ent.pack(side="left", padx=5)
                entries[env_key] = ent

            btn_row = tk.Frame(frame, bg=COLOR_BG)
            btn_row.pack(fill="x", padx=10, pady=(2, 5))
            tk.Button(
                btn_row, text="저장", bg=COLOR_DATALAB, fg="white",
                font=("맑은 고딕", 9), width=6,
                command=lambda ents=entries: self._save_keys(ents)
            ).pack(side="left", padx=2)
            if num > 1:
                tk.Button(
                    btn_row, text="삭제", bg=COLOR_ERR, fg="white",
                    font=("맑은 고딕", 9), width=6,
                    command=lambda ents=entries: self._delete_keys(ents)
                ).pack(side="left", padx=2)

            self.dl_entries.append(entries)

        # 추가 버튼
        add_frame = tk.Frame(scroll_frame, bg=COLOR_BG)
        add_frame.pack(fill="x", padx=5, pady=10)
        tk.Button(
            add_frame, text="+ 데이터랩 키 추가", bg=COLOR_DATALAB, fg="white",
            font=("맑은 고딕", 10, "bold"), width=20,
            command=lambda: self._add_datalab_key(len(dl_keys) + 1)
        ).pack()

    # ── 셀러센터 탭 ────────────────────────────────────────
    def _build_seller_tab(self, keys: OrderedDict):
        for w in self.tab_seller.winfo_children():
            w.destroy()

        desc = tk.Label(
            self.tab_seller, bg=COLOR_BG, anchor="w", justify="left",
            font=("맑은 고딕", 9), fg="#666",
            text="셀러센터 로그인 정보 (중복단어 체크 API 쿠키 자동 수집용)"
        )
        desc.pack(fill="x", padx=10, pady=(10, 5))

        frame = tk.LabelFrame(
            self.tab_seller, text=" 셀러센터 계정 ",
            font=("맑은 고딕", 10, "bold"), bg=COLOR_BG,
            fg=COLOR_SELLER, bd=1, relief="groove"
        )
        frame.pack(fill="x", padx=15, pady=10)

        self.seller_entries = {}
        seller_id = keys.get("NAVER_SELLER_ID", "")
        seller_pw = keys.get("NAVER_SELLER_PW", "")

        for label, val, env_key, show in [
            ("아이디 (이메일)", seller_id, "NAVER_SELLER_ID", None),
            ("비밀번호", seller_pw, "NAVER_SELLER_PW", "*"),
        ]:
            row = tk.Frame(frame, bg=COLOR_BG)
            row.pack(fill="x", padx=10, pady=4)
            tk.Label(row, text=label, bg=COLOR_BG, font=("맑은 고딕", 9), width=14, anchor="w").pack(side="left")
            ent = tk.Entry(row, font=("Consolas", 9), width=45, show=show if show else "")
            ent.insert(0, val)
            ent.pack(side="left", padx=5)
            self.seller_entries[env_key] = ent

        btn_row = tk.Frame(frame, bg=COLOR_BG)
        btn_row.pack(fill="x", padx=10, pady=(2, 8))
        tk.Button(
            btn_row, text="저장", bg=COLOR_SELLER, fg="white",
            font=("맑은 고딕", 9), width=6,
            command=lambda: self._save_keys(self.seller_entries)
        ).pack(side="left", padx=2)

    # ── 키 추출 헬퍼 ──────────────────────────────────────
    def _extract_ad_keys(self, env: OrderedDict) -> list[tuple[str, str, str]]:
        result = []
        cid = env.get("NAVER_AD_CUSTOMER_ID", "")
        akey = env.get("NAVER_AD_API_KEY", "")
        skey = env.get("NAVER_AD_SECRET_KEY", "")
        if cid or akey or skey:
            result.append((cid, akey, skey))
        for i in range(2, 100):
            cid = env.get(f"NAVER_AD_CUSTOMER_ID_{i}", "")
            akey = env.get(f"NAVER_AD_API_KEY_{i}", "")
            skey = env.get(f"NAVER_AD_SECRET_KEY_{i}", "")
            if cid or akey or skey:
                result.append((cid, akey, skey))
            else:
                break
        return result

    def _extract_datalab_keys(self, env: OrderedDict) -> list[tuple[str, str]]:
        result = []
        cid = env.get("NAVER_DATALAB_CLIENT_ID", "")
        csec = env.get("NAVER_DATALAB_CLIENT_SECRET", "")
        if cid or csec:
            result.append((cid, csec))
        for i in range(2, 100):
            cid = env.get(f"NAVER_DATALAB_CLIENT_ID_{i}", "")
            csec = env.get(f"NAVER_DATALAB_CLIENT_SECRET_{i}", "")
            if cid or csec:
                result.append((cid, csec))
            else:
                break
        return result

    # ── 저장/삭제 ─────────────────────────────────────────
    def _save_keys(self, entries: dict[str, tk.Entry]):
        for env_key, ent in entries.items():
            val = ent.get().strip()
            _set_env_value(env_key, val)
        self._set_status("저장 완료", COLOR_OK)
        self._refresh()

    def _delete_keys(self, entries: dict[str, tk.Entry]):
        if not messagebox.askyesno("확인", "이 키를 삭제하시겠습니까?"):
            return
        _remove_env_keys(list(entries.keys()))
        self._set_status("삭제 완료 — 번호 재정렬은 수동으로 해주세요", COLOR_WARN)
        self._refresh()

    def _add_ad_key(self, num: int):
        suffix = f"_{num}" if num > 1 else ""
        for env_key in [
            f"NAVER_AD_CUSTOMER_ID{suffix}",
            f"NAVER_AD_API_KEY{suffix}",
            f"NAVER_AD_SECRET_KEY{suffix}",
        ]:
            _set_env_value(env_key, "")
        self._set_status(f"검색광고 키 #{num} 슬롯 추가됨 — 값을 입력 후 저장하세요", COLOR_WARN)
        self._refresh()

    def _add_datalab_key(self, num: int):
        suffix = f"_{num}" if num > 1 else ""
        for env_key in [
            f"NAVER_DATALAB_CLIENT_ID{suffix}",
            f"NAVER_DATALAB_CLIENT_SECRET{suffix}",
        ]:
            _set_env_value(env_key, "")
        self._set_status(f"데이터랩 키 #{num} 슬롯 추가됨 — 값을 입력 후 저장하세요", COLOR_WARN)
        self._refresh()

    # ── 연결 테스트 ────────────────────────────────────────
    def _test_keys(self):
        self._set_status("API 연결 테스트 중...", "#333")
        self.root.update()
        try:
            sys.path.insert(0, _THIS_DIR)
            from naver_ads_api import check_api_keys, ad_key_pool, naver_key_pool
            # 키 풀 리로드
            ad_key_pool._keys.clear()
            ad_key_pool._idx = 0
            from dotenv import load_dotenv
            load_dotenv(_ENV_PATH, override=True)
            ad_key_pool._load_keys()
            naver_key_pool._keys.clear()
            naver_key_pool._exhausted.clear()
            naver_key_pool._idx = 0
            naver_key_pool._load_keys()

            info = check_api_keys()
            parts = []
            ad_ok = info.get("ad_api", False)
            ad_cnt = info.get("ad_key_count", 0)
            dl_ok = info.get("datalab", False)
            dl_cnt = info.get("datalab_key_count", 0)
            parts.append(f"검색광고: {'OK' if ad_ok else 'X'} ({ad_cnt}개)")
            parts.append(f"데이터랩: {'OK' if dl_ok else 'X'} ({dl_cnt}개)")
            color = COLOR_OK if (ad_ok and dl_ok) else COLOR_WARN
            self._set_status("테스트 완료 -- " + " | ".join(parts), color)
        except Exception as e:
            self._set_status(f"테스트 실패: {str(e)[:80]}", COLOR_ERR)

    # ── 새로고침 ──────────────────────────────────────────
    def _refresh(self):
        keys = _read_env()
        ad_keys = self._extract_ad_keys(keys)
        dl_keys = self._extract_datalab_keys(keys)
        seller = bool(keys.get("NAVER_SELLER_ID"))

        self.lbl_summary.config(
            text=f"검색광고 API: {len(ad_keys)}개  |  "
                 f"데이터랩 오픈API: {len(dl_keys)}개  |  "
                 f"셀러센터: {'설정됨' if seller else '미설정'}"
        )

        self._build_ad_tab(keys)
        self._build_datalab_tab(keys)
        self._build_seller_tab(keys)

    def _set_status(self, text: str, color: str = "#333"):
        self.lbl_status.config(text=text, fg=color)


if __name__ == "__main__":
    app = APIKeyManagerGUI()
    app.run()
