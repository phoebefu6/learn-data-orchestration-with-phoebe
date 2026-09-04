/* orchestra-live.js - the nightly-build simulator for learn-data-orchestration-with-phoebe.
   Thirty deterministic nights of Daybreak's warehouse build. The failure model is a
   teaching simulation (seeded probabilities); the scheduling itself - dependency
   resolution, retries, rerun arithmetic, the 30-day tallies - is genuinely computed.
   Host markup: <div class="orchsim" data-stage="1..6"></div>
   Stages unlock levers cumulatively:
     1 bare cron   2 +DAG dependencies   3 +retries   4 +idempotency
     5 +backfill (passport-gated)   6 +alerting & the retry-10x anti-lever
*/
(function () {
  "use strict";

  /* ---------- deterministic model ---------- */
  function mulberry32(a) {
    return function () {
      a |= 0; a = (a + 0x6D2B79F5) | 0;
      var t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  var TASKS = [
    { id: "extract_orders",    label: "extract orders",    writer: false, dur: 20, at: 120 },
    { id: "extract_payments",  label: "extract payments",  writer: false, dur: 20, at: 120 },
    { id: "extract_inventory", label: "extract inventory", writer: false, dur: 15, at: 120 },
    { id: "clean",             label: "clean",             writer: true,  dur: 25, at: 150, deps: ["extract_orders", "extract_payments", "extract_inventory"] },
    { id: "join",              label: "join",              writer: true,  dur: 30, at: 180, deps: ["clean"] },
    { id: "aggregate",         label: "aggregate",         writer: true,  dur: 30, at: 210, deps: ["join"] },
    { id: "publish",           label: "publish dashboard", writer: true,  dur: 15, at: 240, deps: ["aggregate"] }
  ];
  var SLA_MIN = 420;      /* dashboard due 7:00am */
  var BUG_SHIPPED = 10;   /* aggregate logic bug lives days 10-13 */
  var BUG_FOUND = 14;
  var REAL_FAIL_DAY = 14; /* extract_payments hard-breaks: retries never fix it */

  /* per-day event table, generated once with a fixed seed */
  function genDays() {
    var rng = mulberry32(20260904), days = [];
    for (var d = 1; d <= 30; d++) {
      var ev = { transient: {}, late: rng() < 0.10, spike: rng() < 0.07 };
      TASKS.forEach(function (t) {
        var p = t.writer ? 0.02 : 0.08;
        if (rng() < p) ev.transient[t.id] = true;
      });
      days.push(ev);
    }
    return days;
  }
  var DAYS = genDays();

  /* ---------- one night, really scheduled ---------- */
  /* lv: {dag, retries, idem, backfill, alert, storm} -> night result */
  function runNight(d, lv, forcedKill) {
    var ev = DAYS[d - 1];
    var maxRetries = lv.retries ? (lv.storm ? 10 : 2) : 0;
    var state = {}, finish = {}, trace = [];
    var stale = false, dup = false, anyFail = false;

    TASKS.forEach(function (t) {
      var isReal = (d === REAL_FAIL_DAY && t.id === "extract_payments");
      var isTransient = !!ev.transient[t.id] || (forcedKill === t.id);
      var durMul = ev.spike ? 3.5 : 1;
      var lateAdd = (ev.late && t.id === "extract_orders") ? 90 : 0;

      var start, deps = t.deps || [];
      if (lv.dag) {
        var depOk = deps.every(function (x) { return state[x] === "ok"; });
        if (deps.length && !depOk) { state[t.id] = "skipped"; trace.push([t.label, "skipped - upstream did not succeed"]); return; }
        var readyAt = deps.length ? Math.max.apply(null, deps.map(function (x) { return finish[x]; })) : t.at + lateAdd;
        start = Math.max(t.at, readyAt);
      } else {
        start = t.at; /* cron: fixed time, no questions asked */
        if (deps.length) {
          var fresh = deps.every(function (x) { return state[x] === "ok" && finish[x] <= start; });
          if (!fresh) stale = true; /* runs anyway, on yesterday's or partial data */
        }
      }

      if (isReal) {
        var tries = 1 + maxRetries;
        state[t.id] = "failed"; anyFail = true;
        finish[t.id] = start + durMul * t.dur * tries + (maxRetries ? 15 * maxRetries : 0);
        trace.push([t.label, "FAILED for real" + (maxRetries ? " after " + tries + " attempts" : "") + " - a retry cannot fix a schema break"]);
        return;
      }
      if (isTransient) {
        if (maxRetries > 0) {
          state[t.id] = "ok";
          finish[t.id] = start + durMul * t.dur * 2 + 15; /* one failed attempt + backoff + success */
          if (t.writer && !lv.idem) dup = true; /* the crashed attempt already appended rows */
          trace.push([t.label, "failed once, retry succeeded" + (t.writer && !lv.idem ? " - rows from the dead attempt were appended TWICE" : "")]);
        } else {
          state[t.id] = "failed"; anyFail = true;
          finish[t.id] = start + durMul * t.dur;
          trace.push([t.label, "failed - no retry configured"]);
          if (!lv.dag) stale = true;
        }
        return;
      }
      state[t.id] = "ok";
      finish[t.id] = start + durMul * t.dur + lateAdd * (lv.dag ? 0 : 0);
      trace.push([t.label, "ok" + (ev.spike ? " (slow: volume spike)" : "")]);
    });

    var published = lv.dag ? state.publish === "ok" : true; /* cron always publishes something */
    var doneAt = published ? (finish.publish || 0) : null;
    var onTime = published && doneAt <= SLA_MIN;
    var bug = d >= BUG_SHIPPED && d < BUG_FOUND;
    var wrong = published && (bug || (!lv.dag && (stale || ev.late || ev.spike || anyFail)) || dup);
    return { published: published, onTime: onTime, wrong: wrong, missing: !published,
             late: published && !onTime, doneAt: doneAt, trace: trace, dup: dup };
  }

  function runMonth(lv) {
    var r = { onTime: 0, wrong: 0, missing: 0, late: 0, dupDays: 0, days: [] };
    for (var d = 1; d <= 30; d++) {
      var n = runNight(d, lv);
      var cls = n.missing ? "miss" : n.wrong ? "wrong" : n.onTime ? "good" : "late";
      if (n.wrong) r.wrong++;
      if (n.missing) r.missing++;
      else if (n.onTime) r.onTime++;
      else r.late++;
      if (n.dup) r.dupDays++;
      r.days.push({ d: d, cls: cls });
    }
    /* history correctness: the day 10-13 bug stays in the tables until a backfill restates it */
    r.history = 30 - (lv.backfill && lv.idem ? 0 : (BUG_FOUND - BUG_SHIPPED));
    /* time to detect the day-14 hard failure */
    r.detect = !lv.alert ? "9h 4m - a person noticed at 11:04am"
             : lv.storm ? "3h 28m - ten retries ran before the page fired"
             : "12 min - paged at 2:12am";
    r.detectBad = !lv.alert || lv.storm;
    return r;
  }

  /* ---------- passport ---------- */
  function stamps() {
    try { return Object.keys(JSON.parse(localStorage.getItem("lwp-passport:data-orchestration") || "{}")).length; }
    catch (e) { return 0; }
  }

  /* ---------- rendering ---------- */
  function el(tag, cls, txt) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (txt != null) n.textContent = txt;
    return n;
  }

  var LEVERS = [
    { key: "dag",     stage: 2, name: "DAG dependencies",  sub: "each task waits for its upstream to SUCCEED instead of trusting the clock" },
    { key: "retries", stage: 3, name: "Retries with backoff", sub: "two more attempts on failure, spaced out - transient blips recover on their own" },
    { key: "idem",    stage: 4, name: "Idempotent tasks",  sub: "overwrite the night's partition instead of appending - reruns stop double-writing" },
    { key: "backfill", stage: 5, name: "Backfill the bug window", sub: "after the day-14 fix, recompute days 10-13 from the source data", gated: true },
    { key: "alert",   stage: 6, name: "Alert on outcomes",  sub: "page when the dashboard is late or a run dies - not when a process merely restarts" },
    { key: "storm",   stage: 6, name: "Retry everything 10x", sub: "crank retries to ten on every task, just to be safe", anti: true }
  ];

  function build(host) {
    var stage = parseInt(host.getAttribute("data-stage") || "6", 10);
    var lv = { dag: false, retries: false, idem: false, backfill: false, alert: false, storm: false };

    var wk = el("div", "wk orchsim-wk");
    var head = el("div", "wk-head");
    head.appendChild(el("strong", null, "🌙 Thirty nights at Daybreak"));
    head.appendChild(el("span", "oc-badge oc-measured", "measured"));
    head.appendChild(el("span", "oc-headsub", "the failure model is seeded simulation - the scheduling arithmetic is computed live"));
    wk.appendChild(head);

    var body = el("div", "wk-body");

    var hl = el("div", "oc-headline");
    var boxA = el("div", "oc-hbox");
    boxA.appendChild(el("span", "oc-hlab", "Mornings on time"));
    var vA = el("span", "oc-hval"); boxA.appendChild(vA);
    var boxB = el("div", "oc-hbox");
    boxB.appendChild(el("span", "oc-hlab", "Mornings silently wrong"));
    var vB = el("span", "oc-hval"); boxB.appendChild(vB);
    hl.appendChild(boxA); hl.appendChild(boxB);
    body.appendChild(hl);

    /* levers */
    var lvWrap = el("div", "oc-levers"), inputs = {};
    LEVERS.forEach(function (L) {
      if (L.stage > stage) return;
      var row = el("label", "oc-lever" + (L.anti ? " oc-anti" : ""));
      var cb = document.createElement("input");
      cb.type = "checkbox"; inputs[L.key] = cb;
      row.appendChild(cb);
      var tx = el("span", "oc-ltext");
      tx.appendChild(el("b", null, (L.anti ? "⚠ " : "") + L.name));
      tx.appendChild(el("i", null, L.sub));
      row.appendChild(tx);
      if (L.gated && stamps() < 4) {
        cb.disabled = true;
        var lock = el("span", "oc-lock", "🎫 needs 4 passport stamps");
        var bypass = el("a", "oc-bypass", "unlock anyway");
        bypass.href = "#";
        bypass.addEventListener("click", function (e) { e.preventDefault(); cb.disabled = false; lock.remove(); bypass.remove(); });
        row.appendChild(lock); row.appendChild(bypass);
      }
      cb.addEventListener("change", update);
      lvWrap.appendChild(row);
    });
    body.appendChild(lvWrap);

    /* calendar strip */
    var calWrap = el("div", "oc-cal");
    body.appendChild(calWrap);
    var legend = el("div", "oc-callegend");
    [["good", "on time and right"], ["late", "late but right"], ["wrong", "published and silently wrong"], ["miss", "never published"]].forEach(function (p) {
      var s = el("span", "oc-lg");
      s.appendChild(el("i", "oc-sq " + p[0]));
      s.appendChild(document.createTextNode(p[1]));
      legend.appendChild(s);
    });
    body.appendChild(legend);

    /* counters */
    var counters = el("div", "oc-counters");
    body.appendChild(counters);

    /* kill-a-node */
    var kill = el("div", "oc-kill");
    var klab = el("span", "oc-klab", "Kill a task tonight:");
    kill.appendChild(klab);
    var sel = document.createElement("select");
    sel.className = "oc-sel";
    var opt0 = document.createElement("option");
    opt0.value = ""; opt0.textContent = "choose a task"; sel.appendChild(opt0);
    TASKS.forEach(function (t) {
      var o = document.createElement("option");
      o.value = t.id; o.textContent = t.label; sel.appendChild(o);
    });
    kill.appendChild(sel);
    var kbtn = el("button", "oc-kbtn", "Run tonight's trace");
    kbtn.type = "button";
    kill.appendChild(kbtn);
    body.appendChild(kill);
    var traceBox = el("div", "oc-trace");
    body.appendChild(traceBox);

    var verdict = el("p", "oc-verdict");
    body.appendChild(verdict);

    wk.appendChild(body);
    wk.appendChild(el("div", "wk-foot",
      "Thirty nights, fixed seed: which tasks blip, which source lands late, which night spikes is scripted so every run is comparable. What the scheduler DOES about each of them - waiting, skipping, retrying, rerunning, restating - is computed in front of you. The day-10 logic bug and the day-14 hard failure happen in every mode."));
    host.appendChild(wk);

    kbtn.addEventListener("click", function () {
      if (!sel.value) { traceBox.innerHTML = ""; return; }
      var n = runNight(5, lv, sel.value); /* day 5: an otherwise clean night */
      traceBox.innerHTML = "";
      var h = el("div", "oc-tracehead", "Tonight, with " + sel.options[sel.selectedIndex].text + " killed:");
      traceBox.appendChild(h);
      n.trace.forEach(function (row) {
        var r = el("div", "oc-tracerow" + (/FAILED|failed -|skipped/.test(row[1]) ? " bad" : /retry|TWICE/.test(row[1]) ? " warn" : ""));
        r.appendChild(el("b", null, row[0]));
        r.appendChild(el("span", null, row[1]));
        traceBox.appendChild(r);
      });
      var tail = n.missing ? "No dashboard this morning - and everyone can SEE there is no dashboard."
        : n.wrong ? "The dashboard published anyway. It is wrong, and it looks exactly like every other morning."
        : n.onTime ? "Recovered: the dashboard is on time and right." : "Recovered late: right, after the 7am promise.";
      traceBox.appendChild(el("div", "oc-tracetail", tail));
    });

    function counter(parent, lab, val, warn) {
      var c = el("div", "oc-counter" + (warn ? " warn" : ""));
      c.appendChild(el("b", null, val));
      c.appendChild(el("span", null, lab));
      parent.appendChild(c);
    }

    function update() {
      LEVERS.forEach(function (L) { if (inputs[L.key]) lv[L.key] = inputs[L.key].checked; });
      /* dependencies between levers */
      ["retries"].forEach(function (k) { if (inputs[k]) { inputs[k].disabled = !lv.dag; if (!lv.dag) { lv[k] = false; inputs[k].checked = false; } } });
      if (inputs.idem) { inputs.idem.disabled = !lv.retries; if (!lv.retries) { lv.idem = false; inputs.idem.checked = false; } }
      if (inputs.backfill && !inputs.backfill.disabled) { if (!lv.idem) { lv.backfill = false; inputs.backfill.checked = false; } }
      if (inputs.backfill) inputs.backfill.title = lv.idem ? "" : "Backfilling non-idempotent tasks rewrites history unpredictably - idempotency first.";
      if (inputs.storm) { inputs.storm.disabled = !lv.retries; if (!lv.retries) { lv.storm = false; inputs.storm.checked = false; } }

      var r = runMonth(lv);
      vA.textContent = r.onTime + "/30";
      vA.className = "oc-hval " + (r.onTime >= 28 ? "good" : r.onTime >= 22 ? "mid" : "bad");
      vB.textContent = r.wrong + "/30";
      vB.className = "oc-hval " + (r.wrong === 0 ? "good" : r.wrong <= 4 ? "mid" : "bad");

      calWrap.innerHTML = "";
      r.days.forEach(function (day) {
        var s = el("i", "oc-sq big " + day.cls);
        s.title = "day " + day.d;
        calWrap.appendChild(s);
      });

      counters.innerHTML = "";
      counter(counters, "never published", String(r.missing), r.missing > 0);
      counter(counters, "late but right", String(r.late), false);
      counter(counters, "nights that double-wrote", String(r.dupDays), r.dupDays > 0);
      counter(counters, "days of history correct", r.history + "/30", r.history < 30);
      if (stage >= 6) counter(counters, "time to detect the day-14 break", r.detect, r.detectBad);

      verdict.textContent = verdictText(lv, r);
    }

    function verdictText(lv, r) {
      if (lv.storm) return "Ten retries fixed nothing two retries had not already fixed - transient blips recover on attempt two. What changed: the day-14 hard failure now burns through ten attempts before anyone is paged. Aggressive retries do not add reliability; they add delay in front of your alert.";
      if (lv.alert) return "Same mornings, same numbers - but the day-14 break now pages you at 2:12am instead of being discovered by a person at 11am. Alert on the outcome (no dashboard by 7am, run dead), never on processes restarting.";
      if (lv.backfill && lv.idem) return "Days 10-13 restated from source: history is 30/30 again. Deterministic, idempotent tasks are what made that a button instead of a week of forensic SQL.";
      if (lv.idem) return "Reruns overwrite the night's partition instead of appending to it, so the retried nights stopped double-writing. What remains wrong is the day-10 logic bug - no scheduler can fix code; it can only make the repair cheap (next lever).";
      if (lv.retries) return "Failed mornings came back - transient blips now recover on attempt two. And look at silently-wrong: it went UP. Every retried writer had already appended half its rows before crashing, so the retry wrote them twice. Availability was bought with correctness. The fix is not fewer retries - it is idempotency.";
      if (lv.dag) return "Silently-wrong collapsed: downstream tasks wait for upstream success instead of trusting the clock, so partial data never publishes. The price is honesty - failed nights are now visibly MISSING mornings instead of quietly wrong ones. Nobody is paged yet, and nothing retries.";
      return "Bare cron published thirty mornings out of thirty, on time, every day - and roughly a third of them are wrong. It runs each task at its appointed minute with no idea whether upstream finished, failed, or landed late. Punctuality is not correctness.";
    }

    update();
  }

  document.querySelectorAll(".orchsim").forEach(build);
})();
