#!/usr/bin/env python3
"""Desktop GUI for the Nepal vendor uploader.

This is a thin wrapper around ``fpv_upload.py``: it never re-implements
hashing, duplicate detection, or transport. For every session folder it
shells out to ``fpv_upload.py upload-vendor <folder> --yes`` (the same
command the .cmd scripts run) and parses that command's stdout to drive
per-folder progress bars. All the safety properties (content-hash duplicate
detection, resumability, atomic completion markers) come from that script
unchanged.

Run with:  python3 fpv_upload_gui.py
"""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

SCRIPT_DIR = Path(__file__).resolve().parent
UPLOADER = SCRIPT_DIR / "fpv_upload.py"
STATE_FILE = SCRIPT_DIR / "gui_upload_log.json"
DEST_FILE = SCRIPT_DIR / "gui_destinations.json"

sys.path.insert(0, str(SCRIPT_DIR))
import fpv_upload as backend  # noqa: E402  (local import after sys.path setup)


# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------


@dataclass
class SessionRow:
    """One folder = one upload unit (matches fpv_upload.py's own model:
    the whole selected folder is uploaded as a single preserved tree)."""

    path: Path
    video_count: int = 0
    data_count: int = 0
    total_bytes: int = 0
    status: str = "Pending"          # Pending / Scanning / Uploading / Duplicate / Verified / Failed / Stopped
    uploaded_files: int = 0
    total_files: int = 0
    detail: str = ""
    item_id: str = ""                 # treeview row id
    process: subprocess.Popen | None = field(default=None, repr=False)


def classify_folder(folder: Path) -> tuple[int, int, int]:
    """Quick, non-hashing scan just for the preview table (videos/data/bytes)."""
    videos = 0
    data = 0
    total_bytes = 0
    for path in folder.rglob("*"):
        if not path.is_file():
            continue
        try:
            relative = path.relative_to(folder)
        except ValueError:
            continue
        if backend.is_ignored_card_path(relative):
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        total_bytes += size
        if path.suffix.lower() in backend.VIDEO_EXTENSIONS:
            videos += 1
        else:
            data += 1
    return videos, data, total_bytes


def load_destinations() -> dict:
    """Nested dict of remembered destination folders, e.g. {"SiteA": {"Week1": {}}}.

    This is a LOCAL bookmark list only. The upload token is scoped per-session
    by the broker and grants no permission to list the bucket, so there is no
    way to show what destination folders already exist in R2 -- only what
    this computer has used or created before.
    """
    if DEST_FILE.exists():
        try:
            data = json.loads(DEST_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except (OSError, json.JSONDecodeError):
            pass
    return {}


def save_destinations(tree: dict) -> None:
    try:
        DEST_FILE.write_text(json.dumps(tree, indent=2, sort_keys=True), encoding="utf-8")
    except OSError:
        pass


def sanitize_segment(name: str) -> str:
    return backend.slugify(name.strip(), fallback="")


def load_local_log() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
    return {}


def save_local_log(log: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(log, indent=2, sort_keys=True), encoding="utf-8")
    except OSError:
        pass


# --------------------------------------------------------------------------
# Destination folder picker (local bookmarks only -- see load_destinations)
# --------------------------------------------------------------------------


class DestinationDialog(tk.Toplevel):
    def __init__(self, parent: "UploaderApp", current: str) -> None:
        super().__init__(parent)
        self.title("Choose destination folder")
        self.geometry("480x420")
        self.transient(parent)
        self.grab_set()
        self.result: str | None = None
        self.tree_data = load_destinations()

        ttk.Label(
            self,
            text=(
                "This computer's remembered destination folders.\n"
                "The upload token cannot list the bucket, so this list only shows "
                "folders you have used or created here before — not everything in R2."
            ),
            wraplength=450,
            justify="left",
        ).pack(fill="x", padx=10, pady=(10, 6))

        self.tv = ttk.Treeview(self, show="tree")
        self.tv.pack(fill="both", expand=True, padx=10, pady=6)
        self._populate(self.tree_data, "")

        entry_row = ttk.Frame(self)
        entry_row.pack(fill="x", padx=10, pady=4)
        ttk.Label(entry_row, text="New subfolder name:").pack(side="left")
        self.new_name = tk.StringVar()
        ttk.Entry(entry_row, textvariable=self.new_name).pack(side="left", fill="x", expand=True, padx=6)
        ttk.Button(entry_row, text="Create", command=self._create_folder).pack(side="left")

        button_row = ttk.Frame(self)
        button_row.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(button_row, text="Use root bucket (no subfolder)", command=self._use_root).pack(side="left")
        ttk.Button(button_row, text="Select this folder", command=self._select).pack(side="right")
        ttk.Button(button_row, text="Cancel", command=self.destroy).pack(side="right", padx=6)

        # pre-select current path if it exists in the tree
        if current:
            self._select_path(current.split("/"))

    def _populate(self, node: dict, parent_iid: str) -> None:
        for name in sorted(node):
            iid = self.tv.insert(parent_iid, "end", text=name, open=False)
            self._populate(node[name], iid)

    def _iid_path(self, iid: str) -> list[str]:
        parts = []
        while iid:
            parts.insert(0, self.tv.item(iid, "text"))
            iid = self.tv.parent(iid)
        return parts

    def _select_path(self, parts: list[str]) -> None:
        parts = [p for p in parts if p]
        iid = ""
        for part in parts:
            found = None
            for child in self.tv.get_children(iid):
                if self.tv.item(child, "text") == part:
                    found = child
                    break
            if found is None:
                return
            iid = found
            self.tv.item(iid, open=True)
        if iid:
            self.tv.selection_set(iid)
            self.tv.see(iid)

    def _selected_node_dict(self) -> tuple[dict, list[str]]:
        selection = self.tv.selection()
        if not selection:
            return self.tree_data, []
        path = self._iid_path(selection[0])
        node = self.tree_data
        for part in path:
            node = node[part]
        return node, path

    def _create_folder(self) -> None:
        segment = sanitize_segment(self.new_name.get())
        if not segment:
            messagebox.showwarning("Invalid name", "Enter a folder name using letters, numbers, '.', '_' or '-'.")
            return
        parent_node, parent_path = self._selected_node_dict()
        parent_iid = self.tv.selection()[0] if self.tv.selection() else ""
        if segment in parent_node:
            messagebox.showinfo("Already exists", f"\"{segment}\" already exists there.")
            self._select_path(parent_path + [segment])
            return
        parent_node[segment] = {}
        new_iid = self.tv.insert(parent_iid, "end", text=segment, open=True)
        self.tv.selection_set(new_iid)
        self.tv.see(new_iid)
        self.new_name.set("")
        save_destinations(self.tree_data)

    def _use_root(self) -> None:
        self.result = ""
        self.destroy()

    def _select(self) -> None:
        selection = self.tv.selection()
        if not selection:
            self.result = ""
        else:
            self.result = "/".join(self._iid_path(selection[0]))
        self.destroy()


class UploaderApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("FPV Stereo Nepal — Vendor Uploader")
        self.geometry("1080x680")
        self.minsize(900, 560)

        self.rows: dict[str, SessionRow] = {}
        self.queue_order: list[str] = []
        self.work_queue: "queue.Queue[tuple]" = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.stop_requested = False
        self.local_log = load_local_log()

        self._build_widgets()
        self.after(80, self._poll_queue)

    # -- layout -------------------------------------------------------

    def _build_widgets(self) -> None:
        pad = {"padx": 8, "pady": 6}

        top = ttk.Frame(self)
        top.pack(fill="x", **pad)

        ttk.Label(top, text="Source folder:").grid(row=0, column=0, sticky="w")
        self.source_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.source_var, width=70).grid(row=0, column=1, sticky="we", padx=4)
        ttk.Button(top, text="Browse…", command=self._browse).grid(row=0, column=2)
        top.columnconfigure(1, weight=1)

        self.bulk_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            top,
            text="Bulk mode: this folder contains many session folders (one row per subfolder)",
            variable=self.bulk_var,
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(4, 0))

        ttk.Label(top, text="Destination folder:").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.destination_var = tk.StringVar(value="")
        self.destination_display = ttk.Entry(top, textvariable=self.destination_var, width=70, state="readonly")
        self.destination_display.grid(row=2, column=1, sticky="we", padx=4, pady=(6, 0))
        ttk.Button(top, text="Browse / Create…", command=self._choose_destination).grid(row=2, column=2, pady=(6, 0))
        ttk.Label(
            top,
            text="Every subfolder found in the source keeps its own name and internal structure, "
                 "nested inside this destination.",
            foreground="#555555",
        ).grid(row=3, column=0, columnspan=3, sticky="w", pady=(2, 0))

        buttons = ttk.Frame(self)
        buttons.pack(fill="x", **pad)
        ttk.Button(buttons, text="Scan", command=self._scan).pack(side="left")
        self.start_btn = ttk.Button(buttons, text="Start Upload", command=self._start_upload, state="disabled")
        self.start_btn.pack(side="left", padx=6)
        self.stop_btn = ttk.Button(buttons, text="Stop", command=self._stop_upload, state="disabled")
        self.stop_btn.pack(side="left")
        ttk.Button(buttons, text="Run Doctor (check setup)", command=self._run_doctor).pack(side="left", padx=6)
        ttk.Button(buttons, text="Clear finished rows", command=self._clear_finished).pack(side="left")

        # overall progress
        overall = ttk.Frame(self)
        overall.pack(fill="x", **pad)
        ttk.Label(overall, text="Overall:").pack(side="left")
        self.overall_progress = ttk.Progressbar(overall, mode="determinate", maximum=100)
        self.overall_progress.pack(side="left", fill="x", expand=True, padx=8)
        self.overall_label = ttk.Label(overall, text="0 / 0 folders")
        self.overall_label.pack(side="left")

        # table
        columns = ("folder", "videos", "data", "size", "status", "progress")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=14)
        headings = {
            "folder": "Session folder",
            "videos": "Videos",
            "data": "Data files",
            "size": "Size",
            "status": "Status",
            "progress": "Files uploaded",
        }
        widths = {"folder": 320, "videos": 70, "data": 80, "size": 100, "status": 110, "progress": 140}
        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=widths[col], anchor="w")
        self.tree.pack(fill="both", expand=True, padx=8, pady=(0, 6))
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # log pane
        ttk.Label(self, text="Log for selected folder:").pack(anchor="w", padx=8)
        self.log_text = tk.Text(self, height=10, state="disabled", wrap="word")
        self.log_text.pack(fill="both", expand=False, padx=8, pady=(0, 8))

        self.status_bar = ttk.Label(self, text="Idle.", anchor="w")
        self.status_bar.pack(fill="x", side="bottom", padx=8, pady=4)

    # -- scanning -------------------------------------------------------

    def _browse(self) -> None:
        folder = filedialog.askdirectory(title="Select source folder")
        if folder:
            self.source_var.set(folder)

    def _choose_destination(self) -> None:
        dialog = DestinationDialog(self, self.destination_var.get())
        self.wait_window(dialog)
        if dialog.result is not None:
            self.destination_var.set(dialog.result)

    def _scan(self) -> None:
        source_text = self.source_var.get().strip()
        if not source_text:
            messagebox.showwarning("No folder", "Choose a source folder first.")
            return
        source = Path(source_text).expanduser()
        if not source.is_dir():
            messagebox.showerror("Not found", f"{source} is not a directory.")
            return

        self.tree.delete(*self.tree.get_children())
        self.rows.clear()
        self.queue_order.clear()

        if self.bulk_var.get():
            candidates = sorted(
                (p for p in source.iterdir() if p.is_dir() and not p.name.startswith(".")),
                key=lambda p: p.name.casefold(),
            )
            if not candidates:
                messagebox.showwarning("Empty", "No subfolders found. Uncheck bulk mode to upload this folder directly.")
                return
        else:
            candidates = [source]

        self.status_bar.config(text=f"Scanning {len(candidates)} folder(s)…")
        self.update_idletasks()

        for folder in candidates:
            videos, data, total_bytes = classify_folder(folder)
            if videos == 0 and data == 0:
                continue
            row = SessionRow(path=folder, video_count=videos, data_count=data, total_bytes=total_bytes)
            row.total_files = videos + data
            key = str(folder)
            logged = self.local_log.get(key)
            if logged and logged.get("status") == "Verified":
                row.status = "Verified"
                row.detail = "Previously verified on this computer."
            item_id = self.tree.insert(
                "",
                "end",
                values=(
                    folder.name,
                    videos,
                    data,
                    backend.human_bytes(total_bytes),
                    row.status,
                    f"0 / {row.total_files}" if row.status != "Verified" else f"{row.total_files} / {row.total_files}",
                ),
            )
            row.item_id = item_id
            self.rows[item_id] = row
            self.queue_order.append(item_id)

        self.status_bar.config(text=f"Scanned. {len(self.rows)} session folder(s) ready.")
        self.start_btn.config(state="normal" if self.rows else "disabled")
        self._update_overall()

    # -- upload orchestration -------------------------------------------

    def _start_upload(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            return
        self.stop_requested = False
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.worker_thread = threading.Thread(target=self._run_queue, daemon=True)
        self.worker_thread.start()

    def _stop_upload(self) -> None:
        self.stop_requested = True
        self.status_bar.config(text="Stopping after the current file batch… (already-uploaded folders stay safe)")
        for row in self.rows.values():
            if row.process is not None and row.process.poll() is None:
                row.process.terminate()

    def _run_queue(self) -> None:
        for item_id in self.queue_order:
            if self.stop_requested:
                break
            row = self.rows[item_id]
            if row.status == "Verified":
                continue
            self._upload_one(row)
        self.work_queue.put(("queue-done", None, None))

    def _upload_one(self, row: SessionRow) -> None:
        self.work_queue.put(("status", row.item_id, "Uploading"))
        cmd = [
            sys.executable,
            str(UPLOADER),
            "upload-vendor",
            str(row.path),
            "--yes",
        ]
        destination = self.destination_var.get().strip()
        if destination:
            cmd += ["--remote", f"r2:fpv-stereo-nepal/{destination}"]
        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(SCRIPT_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            self.work_queue.put(("status", row.item_id, "Failed"))
            self.work_queue.put(("log", row.item_id, f"Could not start uploader: {exc}"))
            return

        row.process = process
        uploaded = 0
        final_status = "Failed"
        assert process.stdout is not None
        for line in process.stdout:
            line = line.rstrip("\n")
            self.work_queue.put(("log", row.item_id, line))
            if line.startswith("UPLOADED —"):
                uploaded += 1
                self.work_queue.put(("progress", row.item_id, uploaded))
            elif line.startswith("DUPLICATE"):
                final_status = "Duplicate"
            elif line.startswith("VERIFIED"):
                final_status = "Verified"
            elif line.startswith("ERROR:"):
                final_status = "Failed"

        return_code = process.wait()
        row.process = None
        if return_code != 0 and final_status not in {"Verified", "Duplicate"}:
            final_status = "Stopped" if self.stop_requested else "Failed"
        elif final_status == "Duplicate":
            final_status = "Verified"

        self.work_queue.put(("status", row.item_id, final_status))
        if final_status == "Verified":
            self.local_log[str(row.path)] = {"status": "Verified", "when": time.time()}
            save_local_log(self.local_log)

    # -- queue polling / UI updates --------------------------------------

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, item_id, payload = self.work_queue.get_nowait()
                if kind == "status":
                    self._set_status(item_id, payload)
                elif kind == "progress":
                    self._set_progress(item_id, payload)
                elif kind == "log":
                    self.rows[item_id].detail = payload
                    if self.tree.selection() == (item_id,):
                        self._append_log(payload)
                elif kind == "queue-done":
                    self.start_btn.config(state="normal")
                    self.stop_btn.config(state="disabled")
                    self.status_bar.config(text="Done." if not self.stop_requested else "Stopped.")
        except queue.Empty:
            pass
        self.after(80, self._poll_queue)

    def _set_status(self, item_id: str, status: str) -> None:
        row = self.rows[item_id]
        row.status = status
        values = list(self.tree.item(item_id, "values"))
        values[4] = status
        self.tree.item(item_id, values=values)
        self._update_overall()

    def _set_progress(self, item_id: str, uploaded: int) -> None:
        row = self.rows[item_id]
        row.uploaded_files = uploaded
        values = list(self.tree.item(item_id, "values"))
        values[5] = f"{uploaded} / {row.total_files}"
        self.tree.item(item_id, values=values)
        self._update_overall()

    def _update_overall(self) -> None:
        total = len(self.rows)
        done = sum(1 for r in self.rows.values() if r.status in {"Verified", "Failed", "Stopped"})
        self.overall_label.config(text=f"{done} / {total} folders")
        self.overall_progress["maximum"] = max(total, 1)
        self.overall_progress["value"] = done

    def _on_select(self, _event=None) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        item_id = selection[0]
        row = self.rows.get(item_id)
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        if row and row.detail:
            self.log_text.insert("end", row.detail + "\n")
        self.log_text.config(state="disabled")

    def _append_log(self, line: str) -> None:
        self.log_text.config(state="normal")
        self.log_text.insert("end", line + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")

    def _clear_finished(self) -> None:
        for item_id in list(self.rows):
            row = self.rows[item_id]
            if row.status in {"Verified", "Failed", "Stopped"}:
                self.tree.delete(item_id)
                del self.rows[item_id]
                self.queue_order.remove(item_id)
        self._update_overall()

    def _run_doctor(self) -> None:
        def worker() -> None:
            try:
                result = subprocess.run(
                    [sys.executable, str(UPLOADER), "doctor"],
                    cwd=str(SCRIPT_DIR),
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                output = result.stdout + result.stderr
            except (OSError, subprocess.TimeoutExpired) as exc:
                output = f"Could not run doctor: {exc}"
            self.after(0, lambda: messagebox.showinfo("Doctor", output))

        threading.Thread(target=worker, daemon=True).start()


def main() -> int:
    if not UPLOADER.exists():
        print(f"fpv_upload.py not found next to this script: {UPLOADER}", file=sys.stderr)
        return 2
    app = UploaderApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
