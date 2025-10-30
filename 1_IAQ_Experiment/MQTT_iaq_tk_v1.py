#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IAQ MQTT Label Logger (Tkinter)
- Replaces USB serial reader with MQTT subscriber
- Subscribes to JSON telemetry from ESP32 (topic default: esp32/iaq/telemetry)
- Live dashboard + CO₂ plot + CSV logging with condition labels
- Designed to match your previous CSV column names/order

Payload expected (from ESP32 sketch):
{
  "ms":123456,
  "co2_ppm":754,
  "temp_scd":24.1,
  "hum_scd":48.3,
  "temp_bme":23.8,
  "hum_bme":49.2,
  "press_hpa":1008.6,
  "gas_ohm":83214,
  "iaq_index":63.2,
  "iaq_status":"Good",
  "co2_status":"Normal"
}
"""

import threading
import queue
import time
import os
import csv
from datetime import datetime
import json
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# MQTT
try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

# Matplotlib (optional)
try:
    import matplotlib
    matplotlib.use("TkAgg")
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
except Exception:
    matplotlib = None
    Figure = None
    FigureCanvasTkAgg = None


# ---------- Helpers ----------
def parse_flexible(s):
    """Accept float, int, None, or string with dot/comma decimal."""
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except:
        pass
    try:
        return float(s.replace(",", "."))
    except:
        return 0.0


def nice_num(x: float, round_: bool) -> float:
    import math
    if x <= 0:
        return 1.0
    exp = math.floor(math.log10(x))
    f = x / (10 ** exp)
    if round_:
        if f < 1.5: nf = 1
        elif f < 3: nf = 2
        elif f < 7: nf = 5
        else: nf = 10
    else:
        if f <= 1: nf = 1
        elif f <= 2: nf = 2
        elif f <= 5: nf = 5
        else: nf = 10
    return nf * (10 ** exp)


def nice_scale(minv: float, maxv: float, max_ticks: int = 5):
    import math
    if maxv <= minv:
        maxv = minv + 1
    rng = nice_num(maxv - minv, False)
    tick = nice_num(rng / (max_ticks - 1), True)
    nice_min = math.floor(minv / tick) * tick
    nice_max = math.ceil(maxv / tick) * tick
    if nice_min > 400 and (nice_min - 400) < tick:
        nice_min = 400
    return nice_min, nice_max, tick


# ---------- Dashboard popup ----------
class DashboardWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("IAQ Dashboard")
        self.configure(bg="black")
        self.resizable(False, False)
        self.geometry("300x380+100+100")

        # Colors
        self.orange = "#996400"
        self.darkblue = "#000064"
        self.grey = "#C6C6C6"
        self.green = "#00A000"
        self.yellow = "#A0A000"
        self.red = "#A00000"

        # Header
        hdr = tk.Frame(self, bg=self.orange, height=33, bd=1, relief="ridge")
        hdr.pack(fill="x", padx=5, pady=(10, 0))
        tk.Label(hdr, text="IAQ Dashboard", fg=self.darkblue, bg=self.orange,
                 font=("Segoe UI", 16, "bold")).pack(pady=2)

        def section(title, top_pad):
            f = tk.Frame(self, bg=self.orange, height=20, bd=1, relief="ridge")
            f.pack(fill="x", padx=5, pady=(top_pad, 0))
            tk.Label(f, text=title, fg=self.darkblue, bg=self.orange,
                     font=("Segoe UI", 12, "bold")).pack()
        section("[SCD41]", 10)

        # Column headers
        grid = tk.Frame(self, bg="black")
        grid.pack(fill="x", padx=10, pady=(4,0))
        for i, t in enumerate(("Label", "Value", "Unit")):
            tk.Label(grid, text=t, fg="white", bg="black",
                     font=("Segoe UI", 9, "bold")).grid(row=0, column=i, sticky="w",
                                                        padx=(0 if i==0 else 12, 0))

        def row(lbltxt, unit, rindex, varname):
            tk.Label(grid, text=lbltxt, fg="white", bg="black",
                     font=("Segoe UI", 12)).grid(row=rindex, column=0, sticky="w")
            v = tk.Label(grid, text="--", fg="white", bg="black", font=("Segoe UI", 12))
            v.grid(row=rindex, column=1, sticky="w", padx=(12,0))
            tk.Label(grid, text=unit, fg=self.grey, bg="black",
                     font=("Segoe UI", 12)).grid(row=rindex, column=2, sticky="w", padx=(12,0))
            setattr(self, varname, v)

        row("CO2 :", "ppm", 1, "co2_val")
        section("[BME680]", 6)

        grid2 = tk.Frame(self, bg="black")
        grid2.pack(fill="x", padx=10, pady=(4,0))
        for i, t in enumerate(("Label", "Value", "Unit")):
            tk.Label(grid2, text=t, fg="white", bg="black",
                     font=("Segoe UI", 9, "bold")).grid(row=0, column=i, sticky="w",
                                                        padx=(0 if i==0 else 12, 0))

        def row2(parent, lbl, unit, r, name):
            tk.Label(parent, text=lbl, fg="white", bg="black", font=("Segoe UI", 12)).grid(row=r, column=0, sticky="w")
            v = tk.Label(parent, text="--", fg="white", bg="black", font=("Segoe UI", 12))
            v.grid(row=r, column=1, sticky="w", padx=(12,0))
            tk.Label(parent, text=unit, fg=self.grey, bg="black", font=("Segoe UI", 12)).grid(row=r, column=2, sticky="w", padx=(12,0))
            setattr(self, name, v)

        row2(grid2, "T  :", "C",    1, "t_val")
        row2(grid2, "RH :", "%",    2, "rh_val")
        row2(grid2, "P  :", "hPa",  3, "p_val")
        row2(grid2, "Gas:", "kOhm", 4, "gas_val")

        section("[STATUS]", 6)
        grid3 = tk.Frame(self, bg="black")
        grid3.pack(fill="x", padx=10, pady=(4,0))
        tk.Label(grid3, text="Label", fg="white", bg="black", font=("Segoe UI", 9, "bold")).grid(row=0, column=0, sticky="w")
        tk.Label(grid3, text="Value", fg="white", bg="black", font=("Segoe UI", 9, "bold")).grid(row=0, column=1, sticky="w", padx=(12,0))
        tk.Label(grid3, text="IAQ :", fg="white", bg="black", font=("Segoe UI", 12)).grid(row=1, column=0, sticky="w")
        self.iaq_word = tk.Label(grid3, text="--", fg="white", bg="black", font=("Segoe UI", 12, "bold"))
        self.iaq_word.grid(row=1, column=1, sticky="w", padx=(12,0))
        tk.Label(grid3, text="CO2 :", fg="white", bg="black", font=("Segoe UI", 12)).grid(row=2, column=0, sticky="w")
        self.co2_word = tk.Label(grid3, text="--", fg="white", bg="black", font=("Segoe UI", 12, "bold"))
        self.co2_word.grid(row=2, column=1, sticky="w", padx=(12,0))

        self.fig = None
        self.ax = None
        self.canvas = None
        self._co2_hist = []
        if matplotlib:
            self.fig = Figure(figsize=(2.6, 0.9), dpi=100)
            self.ax = self.fig.add_subplot(111)
            self.ax.set_facecolor("#111111")
            self.fig.patch.set_facecolor("#000000")
            self.ax.grid(True, alpha=0.15)
            self.ax.tick_params(axis='x', labelsize=8, colors='white')
            self.ax.tick_params(axis='y', labelsize=8, colors='white')
            for spine in self.ax.spines.values():
                spine.set_color('#AAAAAA')
            self.ax.set_ylabel("ppm", color='lightgray', fontsize=9)
            fr = tk.Frame(self, bg="black")
            fr.pack(fill="x", padx=8, pady=(8,8))
            self.canvas = FigureCanvasTkAgg(self.fig, master=fr)
            self.canvas.get_tk_widget().pack(fill="x")

    def update_dashboard(self, co2ppm, t_bme, rh_bme, p_hpa, gas_ohm):
        # Numbers
        self.co2_val.config(text=f"{int(round(co2ppm))}")
        self.t_val.config(text=f"{t_bme:.1f}")
        self.rh_val.config(text=f"{rh_bme:.1f}")
        self.p_val.config(text=f"{p_hpa:.1f}")
        gas_text = f"{gas_ohm/1000.0:.1f}" if gas_ohm >= 1000.0 else f"{int(round(gas_ohm))}"
        self.gas_val.config(text=gas_text)

        # Status colors
        if gas_ohm >= 80000.0:
            self.iaq_word.config(text="Good", fg="#00A000")
        elif gas_ohm >= 40000.0:
            self.iaq_word.config(text="Moderate", fg="#A0A000")
        else:
            self.iaq_word.config(text="Poor", fg="#A00000")

        if co2ppm < 800.0:
            self.co2_word.config(text="Normal", fg="#00A000")
        elif co2ppm < 1200.0:
            self.co2_word.config(text="Elevated", fg="#A0A000")
        else:
            self.co2_word.config(text="High", fg="#A00000")

    def append_co2_and_redraw(self, co2):
        if not matplotlib:
            return
        if co2 > 0:
            self._co2_hist.append(co2)
            if len(self._co2_hist) > 300:
                self._co2_hist.pop(0)

        self.ax.cla()
        self.ax.set_facecolor("#111111")
        self.ax.grid(True, alpha=0.15)
        self.ax.tick_params(axis='x', labelsize=8, colors='white')
        self.ax.tick_params(axis='y', labelsize=8, colors='white')
        for spine in self.ax.spines.values():
            spine.set_color('#AAAAAA')
        self.ax.set_ylabel("ppm", color='lightgray', fontsize=9)

        if len(self._co2_hist) < 2:
            self.ax.set_title("Waiting for CO₂…", color='lightgray', fontsize=9)
            self.canvas.draw_idle()
            return

        vmin = min(self._co2_hist)
        vmax = max(self._co2_hist)
        pad = max(30.0, 0.08*(vmax - vmin + 1))
        ymin, ymax, _ = nice_scale(max(350.0, vmin - pad), vmax + pad, 3)

        self.ax.set_ylim([ymin, ymax])
        self.ax.plot(self._co2_hist, lw=2, color='deepskyblue')
        self.canvas.draw_idle()


# ---------- Main app (MQTT) ----------
class IAQApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("IAQ MQTT Label Logger (Tkinter)")
        self.geometry("1000x820+80+40")

        # State
        self.q = queue.Queue()
        self.mqtt_client = None
        self.mqtt_connected = False

        self.headers = [
            # Match your previous CSV order (with iso_time first)
            "iso_time","ms","co2_ppm","temp_scd","hum_scd","temp_bme","hum_bme",
            "press_hpa","gas_ohm","iaq_index","iaq_status","co2_status",
            "room","occupancy","window","condition_label"
        ]
        self.log_folder = os.path.join(os.path.expanduser("~"), "Documents")
        self.log_path = None
        self.log_file = None
        self.csv_writer = None
        self.rows_logged = 0

        self.co2_hist = []
        self.MAX_POINTS = 600

        # Latest metrics (for dashboard)
        self.co2ppm = 0.0
        self.t_bme = 0.0
        self.rh_bme = 0.0
        self.p_hpa = 0.0
        self.gas_ohm = 0.0

        self.dashboard = None

        self._build_ui()
        self._init_live_table(self.headers)
        self.after(100, self._poll_queue)

    # ----- UI -----
    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=8)

        # MQTT controls
        ttk.Label(top, text="Broker:").pack(side="left")
        self.broker_var = tk.StringVar(value="localhost")
        ttk.Entry(top, textvariable=self.broker_var, width=18).pack(side="left", padx=(6,10))

        ttk.Label(top, text="Port:").pack(side="left")
        self.port_var = tk.StringVar(value="1883")
        ttk.Entry(top, textvariable=self.port_var, width=6).pack(side="left", padx=(6,10))

        ttk.Label(top, text="Topic:").pack(side="left")
        self.topic_var = tk.StringVar(value="esp32/iaq/telemetry")
        ttk.Entry(top, textvariable=self.topic_var, width=28).pack(side="left", padx=(6,10))

        self.connect_btn = ttk.Button(top, text="Connect", command=self._toggle_connect)
        self.connect_btn.pack(side="left", padx=(0,8))

        ttk.Button(top, text="Choose Log Folder", command=self._choose_folder).pack(side="left")

        ttk.Button(top, text="Open Dashboard", command=self._open_dashboard).pack(side="left", padx=(6,0))

        # Labels group
        labels_gb = ttk.LabelFrame(self, text="Labels")
        labels_gb.pack(fill="x", padx=10, pady=(0,8))
        grid = ttk.Frame(labels_gb)
        grid.pack(fill="x", padx=8, pady=8)

        self.room_choices = ["Bedroom", "LivingRoom", "Kitchen", "Office", "Lab"]
        self.occ_choices = ["Empty", "1 Person", "2 Persons", "3+ Persons"]
        self.win_choices = ["Fully Open", "Small Gap", "Fully Closed", "Trickle", "Fan"]

        ttk.Label(grid, text="Room:").grid(row=0, column=0, sticky="w")
        self.room_var = tk.StringVar(value=self.room_choices[0])
        room_cb = ttk.Combobox(grid, state="readonly", textvariable=self.room_var, values=self.room_choices, width=18)
        room_cb.grid(row=0, column=1, sticky="w", padx=6)
        room_cb.bind("<<ComboboxSelected>>", lambda e: self._update_condition())

        ttk.Label(grid, text="Occupancy:").grid(row=0, column=3, sticky="w")
        self.occ_var = tk.StringVar(value=self.occ_choices[0])
        occ_cb = ttk.Combobox(grid, state="readonly", textvariable=self.occ_var, values=self.occ_choices, width=18)
        occ_cb.grid(row=0, column=4, sticky="w", padx=6)
        occ_cb.bind("<<ComboboxSelected>>", lambda e: self._update_condition())

        ttk.Label(grid, text="Window:").grid(row=0, column=6, sticky="w")
        self.win_var = tk.StringVar(value=self.win_choices[0])
        win_cb = ttk.Combobox(grid, state="readonly", textvariable=self.win_var, values=self.win_choices, width=18)
        win_cb.grid(row=0, column=7, sticky="w", padx=6)
        win_cb.bind("<<ComboboxSelected>>", lambda e: self._update_condition())

        ttk.Label(grid, text="Condition Label:").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.cond_label = ttk.Label(grid, text="")
        self.cond_label.grid(row=1, column=1, columnspan=7, sticky="w", pady=(8,0))
        self._update_condition()

        # Console
        console_gb = ttk.LabelFrame(self, text="MQTT Console (debug)")
        console_gb.pack(fill="x", padx=10, pady=(0,8))
        self.console = tk.Text(console_gb, height=7, font=("Consolas", 10), wrap="none")
        self.console.pack(fill="x", padx=6, pady=6)

        # CO2 graph
        graph_gb = ttk.LabelFrame(self, text="CO₂ Live Graph (ppm)")
        graph_gb.pack(fill="both", expand=False, padx=10, pady=(0,8), ipady=2)
        if matplotlib:
            self.fig = Figure(figsize=(9.0, 2.4), dpi=100)
            self.ax = self.fig.add_subplot(111)
            self.ax.set_facecolor("#0F0F0F")
            self.fig.patch.set_facecolor("#FFFFFF")
            self.ax.grid(True, alpha=0.2)
            self.line, = self.ax.plot([], [], lw=2, color='deepskyblue')
            self.ax.set_ylabel("ppm")
            self.ax.set_xlabel("samples")
            self.canvas = FigureCanvasTkAgg(self.fig, master=graph_gb)
            self.canvas.get_tk_widget().pack(fill="both", expand=True, padx=6, pady=6)
        else:
            ttk.Label(graph_gb, text="Matplotlib not available. Install with: pip install matplotlib").pack(padx=8, pady=8)

        # CSV Live Monitor
        table_gb = ttk.LabelFrame(self, text="CSV Live Monitor")
        table_gb.pack(fill="both", expand=True, padx=10, pady=(0,8))
        self.table = ttk.Treeview(table_gb, show="headings")
        self.table.pack(fill="both", expand=True, padx=6, pady=6)
        self.table_scroll_y = ttk.Scrollbar(table_gb, orient="vertical", command=self.table.yview)
        self.table.configure(yscroll=self.table_scroll_y.set)
        self.table_scroll_y.pack(side="right", fill="y")

        # Footer
        footer = ttk.Frame(self)
        footer.pack(fill="x", padx=10, pady=(0,10))
        self.log_file_lbl = ttk.Label(footer, text=f"Log file: (not started)  —  Folder: {self.log_folder}")
        self.log_file_lbl.pack(side="left")
        ttk.Label(footer, text="   |   ").pack(side="left")
        self.csv_status_lbl = ttk.Label(footer, text="CSV: idle")
        self.csv_status_lbl.pack(side="left")

    def _update_condition(self):
        room = self.room_var.get()
        occ_tag = self.occ_var.get().replace(" ", "")
        win_tag = self.win_var.get().replace(" ", "")
        self.cond_label.config(text=f"{room}_{occ_tag}_{win_tag}")

    def _append_console(self, text):
        self.console.insert("end", text)
        self.console.see("end")

    def _choose_folder(self):
        d = filedialog.askdirectory(initialdir=self.log_folder, title="Choose folder to save CSV logs")
        if d:
            self.log_folder = d
            self.log_file_lbl.config(text=f"Log file: {(self.log_path or '(not started)')}  —  Folder: {self.log_folder}")

    # ----- CSV & table -----
    def _open_new_log_if_needed(self):
        if self.csv_writer is not None:
            return
        try:
            os.makedirs(self.log_folder, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_path = os.path.join(self.log_folder, f"iaq_log_{ts}.csv")
            self.log_file = open(self.log_path, "w", newline="", encoding="utf-8")
            self.csv_writer = csv.writer(self.log_file)
            self.csv_writer.writerow(self.headers)
            self.log_file.flush()
            self.rows_logged = 0
            self.log_file_lbl.config(text=f"Log file: {self.log_path}")
            self.csv_status_lbl.config(text=f"CSV: opened iaq_log_{ts}.csv")
            self._append_console(f"[INFO] Opened CSV: {self.log_path}\n")
        except Exception as e:
            self._append_console(f"[ERROR] Cannot open log file: {e}\n")

    def _init_live_table(self, headers):
        self.table.delete(*self.table.get_children())
        self.table["columns"] = headers
        for h in headers:
            self.table.heading(h, text=h)
            self.table.column(h, width=110, anchor="w")

    # ----- Graph -----
    def _append_co2(self, val):
        if val <= 0:
            return
        self.co2_hist.append(val)
        if len(self.co2_hist) > self.MAX_POINTS:
            self.co2_hist.pop(0)

    def _redraw_graph(self):
        if not matplotlib:
            return
        self.ax.cla()
        self.ax.set_facecolor("#0F0F0F")
        self.ax.grid(True, alpha=0.2)
        self.ax.set_ylabel("ppm")
        self.ax.set_xlabel("samples")
        if len(self.co2_hist) < 2:
            self.ax.set_title("Waiting for CO₂ data…")
            self.canvas.draw_idle()
            return
        vmin = min(self.co2_hist)
        vmax = max(self.co2_hist)
        pad = max(50.0, 0.08 * (vmax - vmin + 1))
        ymin, ymax, _ = nice_scale(max(350.0, vmin - pad), vmax + pad, 5)
        self.ax.set_ylim([ymin, ymax])
        self.ax.plot(self.co2_hist, lw=2, color='deepskyblue')
        self.canvas.draw_idle()

    # ----- MQTT handling -----
    def _toggle_connect(self):
        if not mqtt:
            messagebox.showerror("Error", "paho-mqtt not installed. pip install paho-mqtt")
            return
        if not self.mqtt_connected:
            self._connect_mqtt()
        else:
            self._disconnect_mqtt()

    def _connect_mqtt(self):
        broker = self.broker_var.get().strip() or "localhost"
        try:
            port = int(self.port_var.get().strip())
        except:
            port = 1883
        topic = self.topic_var.get().strip() or "esp32/iaq/telemetry"

        try:
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.on_connect = self._on_mqtt_connect
            self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
            self.mqtt_client.on_message = self._on_mqtt_message
            self.mqtt_client.connect(broker, port, keepalive=30)
            self.mqtt_client.loop_start()
            self.target_topic = topic
            self._append_console(f"[INFO] Connecting MQTT {broker}:{port}, topic='{topic}' …\n")
        except Exception as e:
            self._append_console(f"[ERROR] MQTT connect failed: {e}\n")
            self.mqtt_client = None

    def _disconnect_mqtt(self):
        try:
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
        except:
            pass
        self.mqtt_client = None
        self.mqtt_connected = False
        self.connect_btn.config(text="Connect")
        self._append_console("[INFO] MQTT disconnected\n")

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            self.connect_btn.config(text="Disconnect")
            self._append_console("[INFO] MQTT connected\n")
            try:
                client.subscribe(self.target_topic, qos=0)
                self._append_console(f"[INFO] Subscribed to {self.target_topic}\n")
            except Exception as e:
                self._append_console(f"[ERROR] Subscribe failed: {e}\n")
        else:
            self._append_console(f"[ERROR] MQTT connect rc={rc}\n")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        self.connect_btn.config(text="Connect")
        self._append_console(f"[INFO] MQTT disconnected (rc={rc})\n")

    def _on_mqtt_message(self, client, userdata, msg):
        # Push into GUI thread via queue
        try:
            payload = msg.payload.decode(errors="ignore")
        except:
            payload = ""
        self.q.put(("mqtt_line", f"{msg.topic} {payload}"))

        # Also parse JSON telemetry when topic matches
        if msg.topic == self.target_topic:
            try:
                data = json.loads(payload)
                self.q.put(("telemetry", data))
            except Exception as e:
                self.q.put(("console", f"[WARN] Bad JSON payload: {e}\n"))

    # ----- Queue pump -----
    def _poll_queue(self):
        try:
            while True:
                typ, payload = self.q.get_nowait()
                if typ == "console":
                    self._append_console(payload)
                elif typ == "mqtt_line":
                    self._append_console(payload + "\n")
                elif typ == "telemetry":
                    self._handle_telemetry(payload)
        except queue.Empty:
            pass
        self.after(60, self._poll_queue)

    # ----- Telemetry handling (JSON) -----
    def _handle_telemetry(self, data: dict):
        # Open CSV on first message
        self._open_new_log_if_needed()

        # Extract fields with defaults
        ms         = int(parse_flexible(data.get("ms", 0)))
        co2_ppm    = parse_flexible(data.get("co2_ppm", 0))
        temp_scd   = parse_flexible(data.get("temp_scd", 0))
        hum_scd    = parse_flexible(data.get("hum_scd", 0))
        temp_bme   = parse_flexible(data.get("temp_bme", 0))
        hum_bme    = parse_flexible(data.get("hum_bme", 0))
        press_hpa  = parse_flexible(data.get("press_hpa", 0))
        gas_ohm    = parse_flexible(data.get("gas_ohm", 0))
        iaq_index  = parse_flexible(data.get("iaq_index", 0))
        iaq_status = str(data.get("iaq_status", ""))
        co2_status = str(data.get("co2_status", ""))

        iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        room = self.room_var.get()
        occ_disp = self.occ_var.get()
        win_disp = self.win_var.get()
        cond = f"{room}_{occ_disp.replace(' ','')}_{win_disp.replace(' ','')}"

        row_vals = [
            iso, ms, co2_ppm, temp_scd, hum_scd, temp_bme, hum_bme,
            press_hpa, int(round(gas_ohm)), iaq_index, iaq_status, co2_status,
            room, occ_disp, win_disp, cond
        ]

        # CSV write
        if self.csv_writer is not None:
            try:
                self.csv_writer.writerow(row_vals)
                self.log_file.flush()
                self.rows_logged += 1
                self.csv_status_lbl.config(text=f"CSV: wrote row {self.rows_logged} at {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                self._append_console(f"[ERROR] Failed to write CSV: {e}\n")

        # Update dashboard caches and graph
        try:
            self.co2ppm  = co2_ppm
            self.t_bme   = temp_bme
            self.rh_bme  = hum_bme
            self.p_hpa   = press_hpa
            self.gas_ohm = gas_ohm
        except Exception:
            pass

        if self.dashboard is not None and self.dashboard.winfo_exists():
            try:
                self.dashboard.update_dashboard(self.co2ppm, self.t_bme, self.rh_bme, self.p_hpa, self.gas_ohm)
                self.dashboard.append_co2_and_redraw(self.co2ppm)
            except Exception:
                pass

        # Live plot in main window
        self._append_co2(self.co2ppm)
        self._redraw_graph()

        # Live table
        try:
            self.table.insert("", "end", values=row_vals)
            self.table.yview_moveto(1.0)
        except Exception:
            pass

    # ----- Dashboard -----
    def _open_dashboard(self):
        try:
            if self.dashboard is not None and self.dashboard.winfo_exists():
                try:
                    self.dashboard.deiconify()
                    self.dashboard.lift()
                    self.dashboard.focus_force()
                    self.dashboard.attributes('-topmost', True)
                    self.after(200, lambda: self.dashboard.attributes('-topmost', False))
                except Exception:
                    self.dashboard = None

            if self.dashboard is None or not self.dashboard.winfo_exists():
                self.dashboard = DashboardWindow(self)
                self.dashboard.protocol("WM_DELETE_WINDOW", self._close_dashboard)
                self.dashboard.bind("<Destroy>", lambda e: setattr(self, "dashboard", None))
                try:
                    self.dashboard.update_dashboard(self.co2ppm, self.t_bme, self.rh_bme, self.p_hpa, self.gas_ohm)
                    if self.co2ppm > 0:
                        self.dashboard.append_co2_and_redraw(self.co2ppm)
                except Exception:
                    pass
                self.dashboard.lift()
                self.dashboard.focus_force()
                self.dashboard.attributes('-topmost', True)
                self.after(200, lambda: self.dashboard.attributes('-topmost', False))

        except Exception as ex:
            import traceback
            traceback.print_exc()
            messagebox.showerror("Dashboard Error", f"Could not open dashboard:\n{ex}")

    def _close_dashboard(self):
        try:
            if self.dashboard is not None and self.dashboard.winfo_exists():
                self.dashboard.destroy()
        finally:
            self.dashboard = None


if __name__ == "__main__":
    app = IAQApp()
    app.mainloop()
