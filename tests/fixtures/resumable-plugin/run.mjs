import fs from "node:fs";

const role = process.argv[2];
const input = JSON.parse(fs.readFileSync(".donkeyspace/run-input.json", "utf8"));
const task = input.plugin.task;
const item = input.work_item?.id;
const resumed = input.previous_tasks.some((entry) => entry.human_response);

const write = (path, body) => {
  fs.mkdirSync(path.substring(0, path.lastIndexOf("/")), { recursive: true });
  fs.writeFileSync(path, body);
};

let changed = [];
let outcome = "implemented";
let handoff = null;
let summary = `live fixture completed ${role}/${task}${item ? ` for ${item}` : ""}`;

if (task === "architect") {
  const specs = {
    storage: "# storage\n\nInputs: clk, rst_n, write_data.\n\nOutputs: read_data.\n\nSubmodules: none.\n\nBehavior: synchronous FIFO storage array.\n",
    fifo: "# fifo\n\nInputs: clk, rst_n, push, pop, write_data.\n\nOutputs: read_data, full, empty.\n\nSubmodules: storage.\n\nBehavior: top-level synchronous FIFO control and datapath.\n",
    monitor: "# monitor\n\nInputs: clk, push, pop.\n\nOutputs: occupancy.\n\nSubmodules: none.\n\nBehavior: independent occupancy monitor.\n",
  };
  for (const [id, spec] of Object.entries(specs)) {
    write(`repo/docs/design/blocks/${id}.md`, spec);
  }
  write("repo/docs/design/blocks/index.json", JSON.stringify({
    work_items: [
      { id: "storage", spec: "docs/design/blocks/storage.md", depends_on: [], metadata: { module: "storage" } },
      { id: "fifo", spec: "docs/design/blocks/fifo.md", depends_on: ["storage"], metadata: { module: "fifo" } },
      { id: "monitor", spec: "docs/design/blocks/monitor.md", depends_on: [], metadata: { module: "monitor" } },
    ],
  }));
  changed = Object.keys(specs).map((id) => `docs/design/blocks/${id}.md`).concat("docs/design/blocks/index.json");
} else if (task === "rtl") {
  write(`repo/rtl/${item}.sv`, `module ${item}(input logic clk, input logic rst_n);\nendmodule\n`);
  changed = [`rtl/${item}.sv`];
} else if (task === "dv_prepare") {
  write(`repo/dv/${item}/${item}_tb.sv`, `module ${item}_tb;\nendmodule\n`);
  changed = [`dv/${item}/${item}_tb.sv`];
} else if (task === "dv_verify") {
  write(`repo/dv/${item}/results.txt`, "verification passed\n");
  changed = [`dv/${item}/results.txt`];
} else if (task === "synthesis") {
  write(`repo/synth/${item}/${item}.ys`, `read_verilog -sv rtl/${item}.sv\nsynth\n`);
  changed = [`synth/${item}/${item}.ys`];
  if (item === "storage" && !resumed) {
    outcome = "needs_changes";
    summary = "storage synthesis requires a human-approved reset implementation choice";
    handoff = { target: "rtl", reason: "Choose synchronous active-low reset; reply to authorize that implementation." };
  }
}

fs.writeFileSync(".donkeyspace/run-result.json", JSON.stringify({
  outcome,
  summary,
  confidence: "high",
  risk: "low",
  questions: [],
  tests: [{ name: "fixture task", command: ["true"], status: "passed", exit_code: 0, summary: null }],
  changed_files: changed,
  human_review_reason: null,
  blocked_reason: null,
  handoff,
}));
