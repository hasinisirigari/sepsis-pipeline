import { useState, useEffect } from "react";

const PATIENTS = [
  {
    id: "ICU-2847",
    name: "Patient A",
    age: 67,
    bed: "Bay 3, Bed 12",
    admitTime: "14.5h ago",
    hoursInIcu: 14.5,
    probability: 0.82,
    alert: "RED",
    vitals: { hr: 118, map: 52, rr: 32, spo2: 88, temp: 39.4 },
    labs: { lactate: 5.8, creatinine: 2.9, wbc: 22.1, platelets: 68, bilirubin: 3.4 },
    vasopressor: true,
    trend: [0.31, 0.38, 0.42, 0.51, 0.58, 0.67, 0.72, 0.78, 0.82],
    shap: [
      { feature: "hours_in_icu", value: 0.28 },
      { feature: "lactate", value: 0.22 },
      { feature: "temperature_4h_mean", value: 0.14 },
      { feature: "on_vasopressor", value: 0.11 },
      { feature: "map_current", value: 0.08 },
    ],
  },
  {
    id: "ICU-1923",
    name: "Patient B",
    age: 54,
    bed: "Bay 1, Bed 4",
    admitTime: "6.2h ago",
    hoursInIcu: 6.2,
    probability: 0.63,
    alert: "ORANGE",
    vitals: { hr: 108, map: 61, rr: 26, spo2: 92, temp: 38.8 },
    labs: { lactate: 3.1, creatinine: 1.8, wbc: 16.4, platelets: 112, bilirubin: 1.9 },
    vasopressor: true,
    trend: [0.12, 0.18, 0.29, 0.35, 0.41, 0.48, 0.55, 0.59, 0.63],
    shap: [
      { feature: "temperature_4h_mean", value: 0.19 },
      { feature: "hours_in_icu", value: 0.15 },
      { feature: "heart_rate_1h_mean", value: 0.12 },
      { feature: "lactate", value: 0.09 },
      { feature: "resp_rate_4h_mean", value: 0.06 },
    ],
  },
  {
    id: "ICU-3301",
    name: "Patient C",
    age: 72,
    bed: "Bay 2, Bed 8",
    admitTime: "32.1h ago",
    hoursInIcu: 32.1,
    probability: 0.41,
    alert: "YELLOW",
    vitals: { hr: 94, map: 68, rr: 22, spo2: 95, temp: 38.2 },
    labs: { lactate: 2.1, creatinine: 1.4, wbc: 13.8, platelets: 155, bilirubin: 1.1 },
    vasopressor: false,
    trend: [0.15, 0.19, 0.22, 0.28, 0.31, 0.35, 0.38, 0.40, 0.41],
    shap: [
      { feature: "hours_in_icu", value: 0.14 },
      { feature: "temperature_4h_mean", value: 0.10 },
      { feature: "heart_rate_1h_mean", value: 0.07 },
      { feature: "wbc", value: 0.05 },
      { feature: "resp_rate_1h_mean", value: 0.04 },
    ],
  },
  {
    id: "ICU-4102",
    name: "Patient D",
    age: 45,
    bed: "Bay 4, Bed 15",
    admitTime: "8.7h ago",
    hoursInIcu: 8.7,
    probability: 0.14,
    alert: "GREEN",
    vitals: { hr: 76, map: 82, rr: 16, spo2: 98, temp: 37.1 },
    labs: { lactate: 0.9, creatinine: 0.8, wbc: 8.2, platelets: 245, bilirubin: 0.6 },
    vasopressor: false,
    trend: [0.08, 0.10, 0.11, 0.12, 0.13, 0.14, 0.14, 0.14, 0.14],
    shap: [
      { feature: "hours_in_icu", value: 0.05 },
      { feature: "heart_rate_4h_mean", value: 0.03 },
      { feature: "temperature_4h_mean", value: 0.02 },
      { feature: "resp_rate_1h_mean", value: 0.01 },
      { feature: "spo2_4h_mean", value: 0.01 },
    ],
  },
  {
    id: "ICU-2156",
    name: "Patient E",
    age: 61,
    bed: "Bay 1, Bed 2",
    admitTime: "3.4h ago",
    hoursInIcu: 3.4,
    probability: 0.19,
    alert: "GREEN",
    vitals: { hr: 82, map: 78, rr: 18, spo2: 97, temp: 37.3 },
    labs: { lactate: 1.2, creatinine: 1.0, wbc: 9.5, platelets: 198, bilirubin: 0.8 },
    vasopressor: false,
    trend: [0.06, 0.09, 0.12, 0.14, 0.15, 0.17, 0.18, 0.19, 0.19],
    shap: [
      { feature: "hours_in_icu", value: 0.06 },
      { feature: "temperature_4h_mean", value: 0.04 },
      { feature: "heart_rate_1h_mean", value: 0.03 },
      { feature: "creatinine", value: 0.02 },
      { feature: "resp_rate_4h_mean", value: 0.02 },
    ],
  },
  {
    id: "ICU-3788",
    name: "Patient F",
    age: 78,
    bed: "Bay 3, Bed 11",
    admitTime: "22.0h ago",
    hoursInIcu: 22.0,
    probability: 0.73,
    alert: "ORANGE",
    vitals: { hr: 112, map: 57, rr: 28, spo2: 90, temp: 39.1 },
    labs: { lactate: 4.4, creatinine: 2.3, wbc: 19.7, platelets: 82, bilirubin: 2.7 },
    vasopressor: true,
    trend: [0.22, 0.30, 0.38, 0.45, 0.52, 0.60, 0.65, 0.70, 0.73],
    shap: [
      { feature: "hours_in_icu", value: 0.24 },
      { feature: "lactate", value: 0.18 },
      { feature: "on_vasopressor", value: 0.12 },
      { feature: "temperature_4h_mean", value: 0.09 },
      { feature: "map_current", value: 0.07 },
    ],
  },
];

const ALERT_CONFIG = {
  RED: { bg: "#dc2626", bgLight: "rgba(220,38,38,0.08)", border: "rgba(220,38,38,0.3)", text: "#dc2626", label: "CRITICAL" },
  ORANGE: { bg: "#ea580c", bgLight: "rgba(234,88,12,0.08)", border: "rgba(234,88,12,0.3)", text: "#ea580c", label: "HIGH" },
  YELLOW: { bg: "#ca8a04", bgLight: "rgba(202,138,4,0.08)", border: "rgba(202,138,4,0.3)", text: "#ca8a04", label: "MODERATE" },
  GREEN: { bg: "#16a34a", bgLight: "rgba(22,163,74,0.08)", border: "rgba(22,163,74,0.3)", text: "#16a34a", label: "LOW" },
};

function SparkLine({ data, color, width = 120, height = 32 }) {
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const points = data
    .map((v, i) => {
      const x = (i / (data.length - 1)) * width;
      const y = height - ((v - min) / range) * (height - 4) - 2;
      return `${x},${y}`;
    })
    .join(" ");
  return (
    <svg width={width} height={height} style={{ display: "block" }}>
      <polyline fill="none" stroke={color} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" points={points} />
      <circle cx={(data.length - 1) / (data.length - 1) * width} cy={height - ((data[data.length - 1] - min) / range) * (height - 4) - 2} r="3" fill={color} />
    </svg>
  );
}

function VitalPill({ label, value, unit, critical }) {
  return (
    <div style={{
      display: "flex", flexDirection: "column", alignItems: "center",
      padding: "6px 10px", borderRadius: "8px",
      background: critical ? "rgba(220,38,38,0.06)" : "rgba(148,163,184,0.06)",
      border: `1px solid ${critical ? "rgba(220,38,38,0.15)" : "rgba(148,163,184,0.1)"}`,
      minWidth: "56px",
    }}>
      <span style={{ fontSize: "10px", color: "#94a3b8", letterSpacing: "0.5px", textTransform: "uppercase" }}>{label}</span>
      <span style={{ fontSize: "16px", fontWeight: 700, color: critical ? "#dc2626" : "#e2e8f0", marginTop: "2px" }}>
        {value}
      </span>
      <span style={{ fontSize: "9px", color: "#64748b" }}>{unit}</span>
    </div>
  );
}

function SHAPBar({ feature, value, maxVal }) {
  const w = (value / maxVal) * 100;
  const labels = {
    hours_in_icu: "Time in ICU",
    lactate: "Lactate",
    temperature_4h_mean: "Temp (4h avg)",
    on_vasopressor: "Vasopressor",
    map_current: "MAP",
    heart_rate_1h_mean: "HR (1h avg)",
    heart_rate_4h_mean: "HR (4h avg)",
    resp_rate_4h_mean: "RR (4h avg)",
    resp_rate_1h_mean: "RR (1h avg)",
    wbc: "WBC",
    creatinine: "Creatinine",
    spo2_4h_mean: "SpO2 (4h avg)",
  };
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px" }}>
      <span style={{ fontSize: "11px", color: "#94a3b8", width: "90px", textAlign: "right", flexShrink: 0 }}>
        {labels[feature] || feature}
      </span>
      <div style={{ flex: 1, height: "6px", background: "rgba(148,163,184,0.1)", borderRadius: "3px", overflow: "hidden" }}>
        <div style={{
          width: `${w}%`, height: "100%", borderRadius: "3px",
          background: "linear-gradient(90deg, #f97316, #ef4444)",
          transition: "width 0.6s ease",
        }} />
      </div>
      <span style={{ fontSize: "10px", color: "#64748b", width: "32px", flexShrink: 0 }}>{value.toFixed(2)}</span>
    </div>
  );
}

function PatientCard({ patient, selected, onClick }) {
  const cfg = ALERT_CONFIG[patient.alert];
  return (
    <div
      onClick={onClick}
      style={{
        padding: "14px 16px",
        borderRadius: "12px",
        background: selected ? cfg.bgLight : "rgba(15,23,42,0.6)",
        border: `1px solid ${selected ? cfg.border : "rgba(148,163,184,0.08)"}`,
        cursor: "pointer",
        transition: "all 0.2s ease",
        marginBottom: "8px",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <div style={{
            width: "10px", height: "10px", borderRadius: "50%", background: cfg.bg,
            boxShadow: `0 0 8px ${cfg.bg}60`,
            animation: patient.alert === "RED" ? "pulse 1.5s infinite" : "none",
          }} />
          <div>
            <div style={{ fontSize: "14px", fontWeight: 600, color: "#e2e8f0" }}>{patient.id}</div>
            <div style={{ fontSize: "11px", color: "#64748b" }}>{patient.bed}</div>
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: "20px", fontWeight: 800, color: cfg.text, fontVariantNumeric: "tabular-nums" }}>
            {(patient.probability * 100).toFixed(0)}%
          </div>
          <div style={{
            fontSize: "9px", fontWeight: 700, letterSpacing: "1px",
            color: cfg.text, opacity: 0.8,
          }}>{cfg.label}</div>
        </div>
      </div>
      <div style={{ marginTop: "8px" }}>
        <SparkLine data={patient.trend} color={cfg.text} />
      </div>
    </div>
  );
}

function DetailPanel({ patient }) {
  const cfg = ALERT_CONFIG[patient.alert];
  const maxShap = Math.max(...patient.shap.map(s => s.value));
  return (
    <div style={{
      background: "rgba(15,23,42,0.8)",
      borderRadius: "16px",
      border: `1px solid ${cfg.border}`,
      padding: "24px",
      flex: 1,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "24px" }}>
        <div>
          <div style={{ fontSize: "11px", color: "#64748b", letterSpacing: "1px", textTransform: "uppercase" }}>Patient Detail</div>
          <div style={{ fontSize: "24px", fontWeight: 800, color: "#e2e8f0", marginTop: "4px" }}>{patient.id}</div>
          <div style={{ fontSize: "13px", color: "#94a3b8", marginTop: "2px" }}>
            Age {patient.age} · {patient.bed} · Admitted {patient.admitTime}
          </div>
        </div>
        <div style={{
          padding: "12px 20px", borderRadius: "12px",
          background: cfg.bgLight, border: `1px solid ${cfg.border}`,
          textAlign: "center",
        }}>
          <div style={{ fontSize: "32px", fontWeight: 900, color: cfg.text, lineHeight: 1, fontVariantNumeric: "tabular-nums" }}>
            {(patient.probability * 100).toFixed(1)}%
          </div>
          <div style={{ fontSize: "10px", fontWeight: 700, letterSpacing: "1.5px", color: cfg.text, marginTop: "4px" }}>
            {cfg.label} RISK
          </div>
        </div>
      </div>

      <div style={{ marginBottom: "24px" }}>
        <div style={{ fontSize: "11px", color: "#64748b", letterSpacing: "1px", textTransform: "uppercase", marginBottom: "10px" }}>
          Vital Signs
        </div>
        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
          <VitalPill label="HR" value={patient.vitals.hr} unit="bpm" critical={patient.vitals.hr > 100} />
          <VitalPill label="MAP" value={patient.vitals.map} unit="mmHg" critical={patient.vitals.map < 65} />
          <VitalPill label="RR" value={patient.vitals.rr} unit="/min" critical={patient.vitals.rr > 22} />
          <VitalPill label="SpO₂" value={patient.vitals.spo2} unit="%" critical={patient.vitals.spo2 < 94} />
          <VitalPill label="Temp" value={patient.vitals.temp} unit="°C" critical={patient.vitals.temp > 38.3} />
          {patient.vasopressor && (
            <div style={{
              display: "flex", alignItems: "center", padding: "6px 12px", borderRadius: "8px",
              background: "rgba(220,38,38,0.06)", border: "1px solid rgba(220,38,38,0.15)",
              fontSize: "11px", fontWeight: 700, color: "#dc2626", letterSpacing: "0.5px",
            }}>
              ● VASOPRESSOR
            </div>
          )}
        </div>
      </div>

      <div style={{ marginBottom: "24px" }}>
        <div style={{ fontSize: "11px", color: "#64748b", letterSpacing: "1px", textTransform: "uppercase", marginBottom: "10px" }}>
          Lab Values
        </div>
        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
          <VitalPill label="Lactate" value={patient.labs.lactate} unit="mmol/L" critical={patient.labs.lactate > 2.0} />
          <VitalPill label="Creat" value={patient.labs.creatinine} unit="mg/dL" critical={patient.labs.creatinine > 1.5} />
          <VitalPill label="WBC" value={patient.labs.wbc} unit="K/μL" critical={patient.labs.wbc > 12} />
          <VitalPill label="PLT" value={patient.labs.platelets} unit="K/μL" critical={patient.labs.platelets < 150} />
          <VitalPill label="Bili" value={patient.labs.bilirubin} unit="mg/dL" critical={patient.labs.bilirubin > 1.2} />
        </div>
      </div>

      <div style={{ marginBottom: "24px" }}>
        <div style={{ fontSize: "11px", color: "#64748b", letterSpacing: "1px", textTransform: "uppercase", marginBottom: "10px" }}>
          Risk Trend (2h)
        </div>
        <SparkLine data={patient.trend} color={cfg.text} width={400} height={48} />
      </div>

      <div>
        <div style={{ fontSize: "11px", color: "#64748b", letterSpacing: "1px", textTransform: "uppercase", marginBottom: "10px" }}>
          SHAP — Top Contributing Factors
        </div>
        {patient.shap.map((s, i) => (
          <SHAPBar key={i} feature={s.feature} value={s.value} maxVal={maxShap} />
        ))}
      </div>
    </div>
  );
}

export default function SepsisDashboard() {
  const [selectedId, setSelectedId] = useState(PATIENTS[0].id);
  const [time, setTime] = useState(new Date());

  useEffect(() => {
    const interval = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(interval);
  }, []);

  const selected = PATIENTS.find(p => p.id === selectedId);
  const sorted = [...PATIENTS].sort((a, b) => b.probability - a.probability);

  const counts = { RED: 0, ORANGE: 0, YELLOW: 0, GREEN: 0 };
  PATIENTS.forEach(p => counts[p.alert]++);

  return (
    <div style={{
      minHeight: "100vh",
      background: "linear-gradient(135deg, #0a0e1a 0%, #0f172a 50%, #0a0e1a 100%)",
      color: "#e2e8f0",
      fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
      padding: "24px",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700;800;900&family=IBM+Plex+Mono:wght@400;600&display=swap');
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: rgba(148,163,184,0.2); border-radius: 2px; }
      `}</style>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "24px" }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
            <div style={{
              width: "8px", height: "8px", borderRadius: "50%", background: "#22c55e",
              boxShadow: "0 0 8px rgba(34,197,94,0.5)",
            }} />
            <h1 style={{
              fontSize: "20px", fontWeight: 800, margin: 0, letterSpacing: "-0.5px",
              background: "linear-gradient(135deg, #e2e8f0, #94a3b8)",
              WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent",
            }}>
              Sepsis Early Warning System
            </h1>
          </div>
          <div style={{ fontSize: "12px", color: "#475569", marginTop: "4px", marginLeft: "20px" }}>
            MIMIC-IV · LightGBM · AUROC 0.86 · 15-min scoring windows
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: "18px", fontWeight: 600, fontFamily: "'IBM Plex Mono', monospace", color: "#94a3b8" }}>
            {time.toLocaleTimeString()}
          </div>
          <div style={{ fontSize: "11px", color: "#475569" }}>
            {time.toLocaleDateString(undefined, { weekday: "long", month: "short", day: "numeric" })}
          </div>
        </div>
      </div>

      <div style={{ display: "flex", gap: "12px", marginBottom: "24px" }}>
        {Object.entries(counts).map(([level, count]) => {
          const cfg = ALERT_CONFIG[level];
          return (
            <div key={level} style={{
              flex: 1, padding: "12px 16px", borderRadius: "12px",
              background: cfg.bgLight, border: `1px solid ${cfg.border}`,
              display: "flex", justifyContent: "space-between", alignItems: "center",
            }}>
              <span style={{ fontSize: "11px", fontWeight: 700, letterSpacing: "1px", color: cfg.text }}>{cfg.label}</span>
              <span style={{ fontSize: "24px", fontWeight: 900, color: cfg.text }}>{count}</span>
            </div>
          );
        })}
      </div>

      <div style={{ display: "flex", gap: "20px" }}>
        <div style={{ width: "300px", flexShrink: 0 }}>
          <div style={{ fontSize: "11px", color: "#64748b", letterSpacing: "1px", textTransform: "uppercase", marginBottom: "10px" }}>
            Patients — sorted by risk
          </div>
          <div style={{ maxHeight: "calc(100vh - 200px)", overflowY: "auto", paddingRight: "4px" }}>
            {sorted.map(p => (
              <PatientCard
                key={p.id}
                patient={p}
                selected={selectedId === p.id}
                onClick={() => setSelectedId(p.id)}
              />
            ))}
          </div>
        </div>
        {selected && <DetailPanel patient={selected} />}
      </div>
    </div>
  );
}
