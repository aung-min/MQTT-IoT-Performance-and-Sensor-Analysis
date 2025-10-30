#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MQTT_Qos_Analyzer_GUI.py — Tkinter GUI to:
  - Start/stop subscriber (listen-only)
  - Run local pub↔sub test (same machine)
  - Tail process logs in a console pane (non-blocking readers)
  - Load/Analyze a CSV and plot latency/reliability/throughput/duplicates

Place this file in the same folder as:
  - PubQoS.py
  - SubQoS.py

Dependencies:
  pip install paho-mqtt pandas matplotlib
"""

import os
import sys
import time
import queue
import threading
import subprocess
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import pandas as pd
import numpy as np  # NEW

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


APP_TITLE = "MQTT QoS Analyzer — Integrated GUI"
DEFAULT_CSV = "mqtt_qos_log.csv"


# ----------------------- robust stream readers -----------------------
class _StreamReader(threading.Thread):
    """Reads a text stream line-by-line and pushes lines to a queue."""
    def __init__(self, stream, q: queue.Queue, tag: str, kind: str):
        super().__init__(daemon=True)
        self.stream = stream
        self.q = q
        self.tag = tag
        self.kind = kind

    def run(self):
        try:
            for line in iter(self.stream.readline, ""):
                if not line:
                    break
                self.q.put((self.tag, self.kind, line))
        except Exception as e:
            self.q.put((self.tag, "ERR", f"[reader] {self.kind} error: {e}\n"))


# ------------------------------ app ---------------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1280x900")  # a bit taller to avoid cropping labels

        # State
        self.sub_proc = None
        self.pub_proc = None
        self.log_q = queue.Queue()
        self.df = None
        self.csv_path = os.path.abspath(DEFAULT_CSV)

        self._build_ui()
        self.after(100, self._poll_logs)

    # ---------------- UI ----------------
    def _build_ui(self):
        # Controls
        ctrl = ttk.LabelFrame(self, text="Test Controls")
        ctrl.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        # Variables
        self.broker_var = tk.StringVar(value="127.0.0.1")
        self.port_var = tk.IntVar(value=1883)
        self.topic_var = tk.StringVar(value="test/qos_analyzer")
        self.per_var = tk.IntVar(value=100)
        self.interval_var = tk.DoubleVar(value=0.05)
        self.runid_var = tk.StringVar(value="runA")
        self.csv_var = tk.StringVar(value=self.csv_path)
        self.pubdev_var = tk.StringVar(value="RaspberryPi5")
        self.subdev_var = tk.StringVar(value="RaspberryPi5")
        self.subqos_var = tk.IntVar(value=2)
        self.ctx_var = tk.StringVar(value="sensor=desk,temp=24.5,hum=47.0")

        # Row 1
        r1 = ttk.Frame(ctrl)
        r1.pack(fill=tk.X, padx=6, pady=4)
        ttk.Label(r1, text="Broker").grid(row=0, column=0, sticky="w")
        ttk.Entry(r1, textvariable=self.broker_var, width=16).grid(row=0, column=1, padx=4)
        ttk.Label(r1, text="Port").grid(row=0, column=2, sticky="w")
        ttk.Entry(r1, textvariable=self.port_var, width=6).grid(row=0, column=3, padx=4)
        ttk.Label(r1, text="Topic").grid(row=0, column=4, sticky="w")
        ttk.Entry(r1, textvariable=self.topic_var, width=24).grid(row=0, column=5, padx=4)
        ttk.Label(r1, text="Msgs/QoS").grid(row=0, column=6, sticky="w")
        ttk.Entry(r1, textvariable=self.per_var, width=8).grid(row=0, column=7, padx=4)
        ttk.Label(r1, text="Interval (s)").grid(row=0, column=8, sticky="w")
        ttk.Entry(r1, textvariable=self.interval_var, width=8).grid(row=0, column=9, padx=4)

        # Row 2
        r2 = ttk.Frame(ctrl)
        r2.pack(fill=tk.X, padx=6, pady=4)
        ttk.Label(r2, text="Run ID").grid(row=0, column=0, sticky="w")
        ttk.Entry(r2, textvariable=self.runid_var, width=12).grid(row=0, column=1, padx=4)
        ttk.Label(r2, text="CSV").grid(row=0, column=2, sticky="w")
        ttk.Entry(r2, textvariable=self.csv_var, width=40).grid(row=0, column=3, padx=4)
        ttk.Button(r2, text="Browse…", command=self._pick_csv).grid(row=0, column=4, padx=6)
        ttk.Label(r2, text="Sub QoS").grid(row=0, column=5, sticky="w")
        ttk.Spinbox(r2, from_=0, to=2, textvariable=self.subqos_var, width=4).grid(row=0, column=6, padx=4)
        ttk.Label(r2, text="Pub Dev").grid(row=0, column=7, sticky="w")
        ttk.Entry(r2, textvariable=self.pubdev_var, width=14).grid(row=0, column=8, padx=4)
        ttk.Label(r2, text="Sub Dev").grid(row=0, column=9, sticky="w")
        ttk.Entry(r2, textvariable=self.subdev_var, width=14).grid(row=0, column=10, padx=4)

        # Row 3
        r3 = ttk.Frame(ctrl)
        r3.pack(fill=tk.X, padx=6, pady=4)
        ttk.Label(r3, text="Message Context").grid(row=0, column=0, sticky="w")
        ttk.Entry(r3, textvariable=self.ctx_var, width=80).grid(row=0, column=1, padx=4, sticky="we")
        r3.grid_columnconfigure(1, weight=1)

        # Buttons
        btns = ttk.Frame(ctrl)
        btns.pack(fill=tk.X, padx=6, pady=6)
        ttk.Button(btns, text="▶ Start Subscriber (listen-only)", command=self.start_subscriber).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="▶ Run Local Pub↔Sub Test", command=self.run_local_test).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="⏹ Stop", command=self.stop_all).pack(side=tk.LEFT, padx=4)
        ttk.Separator(btns, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)
        ttk.Button(btns, text="📂 Load CSV", command=self.load_csv).pack(side=tk.LEFT, padx=4)
        ttk.Button(btns, text="📈 Analyze", command=self.analyze_csv).pack(side=tk.LEFT, padx=4)

        # Splitter
        mid = ttk.Panedwindow(self, orient=tk.VERTICAL)
        mid.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Console (child of mid)
        console_frame = ttk.LabelFrame(mid, text="Live Log")
        self.console = tk.Text(console_frame, height=10, wrap="word")
        self.console.pack(fill=tk.BOTH, expand=True)
        mid.add(console_frame, weight=1)

        # Notebook must also be child of mid
        nb = ttk.Notebook(mid)
        mid.add(nb, weight=3)

        # Tabs
        self.summary_frame = ttk.Frame(nb)
        nb.add(self.summary_frame, text="Summary")

        self.plots_frame = ttk.Frame(nb)
        nb.add(self.plots_frame, text="Plots")

        # --- Summary Treeview with scrollbars (grid layout) ---
        self.tree = ttk.Treeview(
            self.summary_frame,
            columns=("qos", "sent_used", "received", "lat_ms", "lat_std_ms", "reliab", "throughput", "dupes", "dup_rate", "avg_size"),
            show="headings"
        )
        for col, txt, w in [
            ("qos", "QoS", 60),
            ("sent_used", "Sent/QoS (used)", 130),  # NEW
            ("received", "Received", 90),
            ("lat_ms", "Mean Latency (ms)", 150),
            ("lat_std_ms", "Std Latency (ms)", 150),
            ("reliab", "Reliability (%)", 130),
            ("throughput", "Throughput (msg/s)", 170),
            ("dupes", "Duplicates", 110),
            ("dup_rate", "Duplicate Rate (%)", 170),
            ("avg_size", "Avg Size (bytes)", 150),
        ]:
            self.tree.heading(col, text=txt)
            self.tree.column(col, width=w, anchor=tk.CENTER)

        # Scrollbars
        scroll_y = ttk.Scrollbar(self.summary_frame, orient=tk.VERTICAL, command=self.tree.yview)
        scroll_x = ttk.Scrollbar(self.summary_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        # Grid placement
        self.summary_frame.grid_rowconfigure(0, weight=1)
        self.summary_frame.grid_columnconfigure(0, weight=1)
        self.tree.grid(row=0, column=0, sticky="nsew", padx=(8,0), pady=(8,0))
        scroll_y.grid(row=0, column=1, sticky="ns", padx=(0,8), pady=(8,0))
        scroll_x.grid(row=1, column=0, sticky="ew", padx=8, pady=(0,8))

        # --- Plot canvases (give extra bottom margin to avoid cropped x labels) ---
        self.fig_latency = Figure(figsize=(4.8, 3.4), dpi=100)
        self.ax_latency = self.fig_latency.add_subplot(111)
        self.fig_latency.subplots_adjust(bottom=0.22)

        self.cv_latency = FigureCanvasTkAgg(self.fig_latency, master=self.plots_frame)
        self.cv_latency.get_tk_widget().grid(row=0, column=0, padx=8, pady=8, sticky="nsew")

        self.fig_reliab = Figure(figsize=(4.8, 3.4), dpi=100)
        self.ax_reliab = self.fig_reliab.add_subplot(111)
        self.fig_reliab.subplots_adjust(bottom=0.22)

        self.cv_reliab = FigureCanvasTkAgg(self.fig_reliab, master=self.plots_frame)
        self.cv_reliab.get_tk_widget().grid(row=0, column=1, padx=8, pady=8, sticky="nsew")

        self.fig_thru = Figure(figsize=(4.8, 3.4), dpi=100)
        self.ax_thru = self.fig_thru.add_subplot(111)
        self.fig_thru.subplots_adjust(bottom=0.22)

        self.cv_thru = FigureCanvasTkAgg(self.fig_thru, master=self.plots_frame)
        self.cv_thru.get_tk_widget().grid(row=1, column=0, padx=8, pady=8, sticky="nsew")

        self.fig_dupe = Figure(figsize=(4.8, 3.4), dpi=100)
        self.ax_dupe = self.fig_dupe.add_subplot(111)
        self.fig_dupe.subplots_adjust(bottom=0.22)

        self.cv_dupe = FigureCanvasTkAgg(self.fig_dupe, master=self.plots_frame)
        self.cv_dupe.get_tk_widget().grid(row=1, column=1, padx=8, pady=8, sticky="nsew")

        self.plots_frame.grid_columnconfigure(0, weight=1)
        self.plots_frame.grid_columnconfigure(1, weight=1)
        self.plots_frame.grid_rowconfigure(0, weight=1)
        self.plots_frame.grid_rowconfigure(1, weight=1)

        # Give the bottom pane more height initially so plots/labels are visible
        self.after(50, lambda: mid.sashpos(0, 250))  # move sash: 0th divider, 250px from top

    # ------------- helpers -------------
    def _pick_csv(self):
        path = filedialog.asksaveasfilename(
            title="Select CSV file",
            initialfile=os.path.basename(self.csv_var.get()),
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if path:
            self.csv_var.set(path)

    def _append_console(self, tag, stream, text):
        self.console.insert(tk.END, f"[{tag}:{stream}] {text}")
        self.console.see(tk.END)

    def _poll_logs(self):
        try:
            while True:
                tag, stream, text = self.log_q.get_nowait()
                self._append_console(tag, stream, text)
        except queue.Empty:
            pass
        self.after(100, self._poll_logs)

    def _spawn(self, args, tag):
        """Start a subprocess (text mode) and attach two stream readers."""
        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1  # line-buffered
        )
        if proc.stdout:
            _StreamReader(proc.stdout, self.log_q, tag=tag, kind="OUT").start()
        if proc.stderr:
            _StreamReader(proc.stderr, self.log_q, tag=tag, kind="ERR").start()
        return proc

    # ------------- actions -------------
    def start_subscriber(self):
        if self.sub_proc and self.sub_proc.poll() is None:
            messagebox.showinfo("Subscriber", "Subscriber already running.")
            return
        out_csv = self.csv_var.get()
        try:
            os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
        except Exception:
            pass
        args = [
            sys.executable, os.path.abspath("SubQoS.py"),
            "--broker", self.broker_var.get(),
            "--port", str(self.port_var.get()),
            "--topic", self.topic_var.get(),
            "--qos", str(self.subqos_var.get()),
            "--out", out_csv,
            "--subdev", self.subdev_var.get(),
        ]
        self._append_console("GUI", "INFO", "Starting Subscriber…\n")
        self.sub_proc = self._spawn(args, tag="SUB")

    def run_local_test(self):
        # Start subscriber first if not running
        if not (self.sub_proc and self.sub_proc.poll() is None):
            self.start_subscriber()
            time.sleep(0.8)  # allow subscription to attach

        # Then start publisher
        if self.pub_proc and self.pub_proc.poll() is None:
            messagebox.showinfo("Publisher", "Publisher already running.")
            return
        args = [
            sys.executable, os.path.abspath("PubQoS.py"),
            "--broker", self.broker_var.get(),
            "--port", str(self.port_var.get()),
            "--topic", self.topic_var.get(),
            "--per", str(self.per_var.get()),
            "--interval", str(self.interval_var.get()),
            "--runid", self.runid_var.get(),
            "--pubdev", self.pubdev_var.get(),
            "--ctx", self.ctx_var.get(),
        ]
        self._append_console("GUI", "INFO", "Starting Local Publisher…\n")
        self.pub_proc = self._spawn(args, tag="PUB")

    def stop_all(self):
        killed = False
        for p, name in [(self.pub_proc, "Publisher"), (self.sub_proc, "Subscriber")]:
            if p and p.poll() is None:
                self._append_console("GUI", "INFO", f"Stopping {name}…\n")
                try:
                    p.terminate()
                    try:
                        p.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        p.kill()
                    killed = True
                except Exception as e:
                    self._append_console("GUI", "ERR", f"Error stopping {name}: {e}\n")
        # clear handles
        self.pub_proc = None
        self.sub_proc = None
        if not killed:
            self._append_console("GUI", "INFO", "No running processes to stop.\n")

    def load_csv(self):
        path = filedialog.askopenfilename(
            title="Open CSV",
            initialfile=os.path.basename(self.csv_var.get()),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if path:
            self.csv_var.set(path)
            self.csv_path = path
            self._append_console("GUI", "INFO", f"Loaded CSV path: {path}\n")

    # ---------- helper to infer "sent per QoS" ----------
    @staticmethod
    def _infer_sent_per_qos(dfq: pd.DataFrame) -> int | None:
        """
        Infer how many messages were sent for this QoS from msg_id.
        Tries:
          - if ids look 0-based: max_id + 1
          - otherwise: unique id count as a lower bound
        Returns None if cannot infer.
        """
        if dfq.empty or "msg_id" not in dfq.columns:
            return None
        ids = pd.to_numeric(dfq["msg_id"], errors="coerce").dropna().astype(int)
        if ids.empty:
            return None
        max_id = int(ids.max())
        min_id = int(ids.min())
        uniq   = int(ids.nunique())
        candidates = []
        if min_id == 0 and max_id >= 0:
            candidates.append(max_id + 1)
        candidates.append(uniq)
        inferred = max(candidates) if candidates else None
        return inferred if (inferred is not None and inferred >= 1) else None

    def analyze_csv(self):
        path = self.csv_var.get()
        if not os.path.exists(path):
            messagebox.showerror("Analyze", f"CSV not found:\n{path}")
            return
        try:
            df = pd.read_csv(path)
        except Exception as e:
            messagebox.showerror("Analyze", f"Could not read CSV:\n{e}")
            return

        required = [
            "run_id","qos","msg_id","topic_name","msg_context","pub_ts","recv_ts",
            "delay_s","delay_ms","declared_size_bytes","wire_size_bytes","pub_device","sub_device"
        ]
        for col in required:
            if col not in df.columns:
                messagebox.showerror("Analyze", f"CSV missing column: {col}")
                return

        # Filter by run_id; if no match, analyze all
        runid = self.runid_var.get()
        dfr = df[df["run_id"] == runid].copy()
        if dfr.empty:
            self._append_console("GUI", "WARN", f"No rows for run_id={runid}; analyzing all rows.\n")
            dfr = df.copy()
        if dfr.empty:
            messagebox.showwarning("Analyze", "CSV has no rows to analyze.")
            return

        # Convert numeric
        for c in ("qos","msg_id","pub_ts","recv_ts","delay_s","delay_ms","declared_size_bytes","wire_size_bytes"):
            dfr[c] = pd.to_numeric(dfr[c], errors="coerce")

        # GUI fallback for reliability denominator
        sent_per_qos_gui = max(1, int(self.per_var.get()))

        # Duplicate counting: count EXTRA copies beyond the first
        dupe_counts = dfr.groupby(["qos", "msg_id"]).size().reset_index(name="Count")
        dupe_extra_by_qos = (
            dupe_counts.assign(extra=lambda x: x["Count"].clip(lower=1) - 1)
                       .groupby("qos")["extra"].sum()
        )

        # Build summary rows per QoS
        summary = []
        plots = dict(latency_ms=[], qos=[], reliability=[], throughput=[], dup_rate=[], avg_size=[])

        for qos in (0, 1, 2):
            qos_i = int(qos)

            dfq = dfr[dfr["qos"] == qos_i].copy()
            # --- infer sent per QoS from msg_id (robustly) ---
            inferred_sent = self._infer_sent_per_qos(dfq)
            sent_used = inferred_sent if (inferred_sent is not None) else sent_per_qos_gui
            if inferred_sent is not None:
                # warn if disagreement >5%
                if abs(sent_used - sent_per_qos_gui) / max(sent_used, 1) > 0.05:
                    self._append_console("GUI", "WARN",
                        f"QoS {qos_i}: inferred sent={inferred_sent} differs from GUI={sent_per_qos_gui}. Using inferred.\n")

            received = int(dfq["msg_id"].nunique())
            mean_ms = float(dfq["delay_ms"].mean() if not dfq.empty else float("nan"))
            std_ms  = float(dfq["delay_ms"].std()  if not dfq.empty else float("nan"))

            # throughput: received / duration (recv_ts range)
            if len(dfq) >= 2:
                duration = float(dfq["recv_ts"].max() - dfq["recv_ts"].min())
                thr = (received / duration) if duration > 0 else float("nan")
            else:
                thr = float("nan")

            reliab = (received / max(1, sent_used)) * 100.0
            dupes = int(dupe_extra_by_qos.get(qos_i, 0))
            dup_rate = (dupes / len(dfq)) * 100.0 if len(dfq) > 0 else 0.0
            avg_size = float(dfq["wire_size_bytes"].mean() if not dfq.empty else float("nan"))

            summary.append((qos_i, sent_used, received, mean_ms, std_ms, reliab, thr, dupes, dup_rate, avg_size))

            plots["qos"].append(qos_i)  # integer ticks on axes
            plots["latency_ms"].append(mean_ms if mean_ms == mean_ms else 0.0)  # NaN-safe
            plots["reliability"].append(reliab)
            plots["throughput"].append(thr if thr == thr else 0.0)
            plots["dup_rate"].append(dup_rate)
            plots["avg_size"].append(avg_size if avg_size == avg_size else 0.0)

        # Fill summary table (clear first)
        for i in self.tree.get_children():
            self.tree.delete(i)
        for (qos_i, sent_used, received, mean_ms, std_ms, reliab, thr, dupes, dup_rate, avg_size) in summary:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    f"{qos_i}",
                    f"{sent_used}",                    # NEW visible denominator
                    received,
                    f"{mean_ms:.2f}" if mean_ms == mean_ms else "-",
                    f"{std_ms:.2f}" if std_ms == std_ms else "-",
                    f"{reliab:.2f}",
                    f"{thr:.3f}" if thr == thr else "-",
                    str(dupes),
                    f"{dup_rate:.2f}",
                    f"{avg_size:.1f}" if avg_size == avg_size else "-"
                ),
                tags=(f"qos{qos_i}",)
            )

        # Draw plots
        self._plot_latency(plots["qos"], plots["latency_ms"])
        self._plot_reliability(plots["qos"], plots["reliability"])
        self._plot_throughput(plots["qos"], plots["throughput"])
        self._plot_dupe(plots["qos"], plots["dup_rate"])

        self._append_console("GUI", "INFO", "Analysis complete.\n")

    # ------------- plotting -------------
    def _plot_latency(self, qos, values):
        self.ax_latency.clear()
        self.ax_latency.bar(qos, values)
        self.ax_latency.set_title("Average Latency vs QoS")
        self.ax_latency.set_xlabel("QoS")
        self.ax_latency.set_ylabel("Mean Latency (ms)")
        self.ax_latency.set_xticks([0, 1, 2])
        self.ax_latency.grid(True)
        self.fig_latency.subplots_adjust(bottom=0.22)  # keep labels visible
        self.cv_latency.draw()

    def _plot_reliability(self, qos, values):
        self.ax_reliab.clear()
        self.ax_reliab.plot(qos, values, marker="o")
        self.ax_reliab.set_title("Reliability vs QoS")
        self.ax_reliab.set_xlabel("QoS")
        self.ax_reliab.set_ylabel("Reliability (%)")
        self.ax_reliab.set_xticks([0, 1, 2])
        self.ax_reliab.set_ylim(0, 105)
        self.ax_reliab.grid(True)
        self.fig_reliab.subplots_adjust(bottom=0.22)
        self.cv_reliab.draw()

    def _plot_throughput(self, qos, values):
        self.ax_thru.clear()
        self.ax_thru.bar(qos, values)
        self.ax_thru.set_title("Throughput vs QoS")
        self.ax_thru.set_xlabel("QoS")
        self.ax_thru.set_ylabel("Messages per second")
        self.ax_thru.set_xticks([0, 1, 2])
        self.ax_thru.grid(True)
        self.fig_thru.subplots_adjust(bottom=0.22)
        self.cv_thru.draw()

    def _plot_dupe(self, qos, values):
        self.ax_dupe.clear()
        self.ax_dupe.bar(qos, values)
        self.ax_dupe.set_title("Duplicate Rate vs QoS")
        self.ax_dupe.set_xlabel("QoS")
        self.ax_dupe.set_ylabel("Duplicate Rate (%)")
        self.ax_dupe.set_xticks([0, 1, 2])
        self.ax_dupe.grid(True)
        self.fig_dupe.subplots_adjust(bottom=0.22)
        self.cv_dupe.draw()


if __name__ == "__main__":
    App().mainloop()
