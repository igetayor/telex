/**
 * Telex Event Streaming Worker Console & DLQ Inspector
 */
(function () {
  'use strict';

  // Elements
  const brokerStatus = document.getElementById('brokerStatus');
  const brokerDot = document.getElementById('brokerDot');
  const workerSaturation = document.getElementById('workerSaturation');
  const metricActiveWorkers = document.getElementById('metricActiveWorkers');
  const workerProgress = document.getElementById('workerProgress');
  const metricThroughput = document.getElementById('metricThroughput');
  const metricQueueDepth = document.getElementById('metricQueueDepth');
  const queueProgress = document.getElementById('queueProgress');
  const metricQueueCap = document.getElementById('metricQueueCap');
  const metricSuccess = document.getElementById('metricSuccess');
  const metricRetried = document.getElementById('metricRetried');
  const metricDLQ = document.getElementById('metricDLQ');
  const metricTotalConsumed = document.getElementById('metricTotalConsumed');
  const dlqBadgeCount = document.getElementById('dlqBadgeCount');

  const eventFeed = document.getElementById('eventFeed');
  const eventCounter = document.getElementById('eventCounter');
  const dlqTableBody = document.getElementById('dlqTableBody');
  const btnClearDLQ = document.getElementById('btnClearDLQ');
  const btnRefreshDLQ = document.getElementById('btnRefreshDLQ');

  const btnInjectNormal = document.getElementById('btnInjectNormal');
  const btnInjectTransient = document.getElementById('btnInjectTransient');
  const btnInjectFatal = document.getElementById('btnInjectFatal');
  const btnInjectTimeout = document.getElementById('btnInjectTimeout');

  const inputEventTopic = document.getElementById('inputEventTopic');
  const inputEventType = document.getElementById('inputEventType');
  const selectSimError = document.getElementById('selectSimError');
  const jsonCustomPayload = document.getElementById('jsonCustomPayload');
  const btnDispatchCustom = document.getElementById('btnDispatchCustom');

  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabContents = document.querySelectorAll('.tab-content');

  let seenEventIDs = new Set();
  let totalRenderedEvents = 0;

  // Initialize
  initTabs();
  initActions();
  startPolling();

  function initTabs() {
    tabButtons.forEach(btn => {
      btn.addEventListener('click', () => {
        tabButtons.forEach(b => b.classList.remove('active'));
        tabContents.forEach(c => c.classList.remove('active'));
        btn.classList.add('active');
        const targetId = btn.getAttribute('data-tab');
        document.getElementById(targetId).classList.add('active');

        if (targetId === 'tab-dlq') {
          fetchDLQ();
        }
      });
    });
  }

  function initActions() {
    btnInjectNormal.addEventListener('click', () => {
      dispatchTestEvent('payment.authorized', '', { amount: 89.95, currency: 'USD', status: 'INITIATED' });
    });

    btnInjectTransient.addEventListener('click', () => {
      dispatchTestEvent('inventory.reserve', 'transient', { sku: 'SKU-772-X', quantity: 4, note: 'Flaky network downstream' });
    });

    btnInjectFatal.addEventListener('click', () => {
      dispatchTestEvent('order.validate', 'fatal', { error_code: 'MALFORMED_HEADER', poison: true });
    });

    btnInjectTimeout.addEventListener('click', () => {
      dispatchTestEvent('rate.limit.test', 'timeout', { client: 'tier-3-bot', requests: 400 });
    });

    btnDispatchCustom.addEventListener('click', () => {
      let payload = {};
      try {
        payload = JSON.parse(jsonCustomPayload.value);
      } catch (err) {
        alert('Invalid JSON in custom payload: ' + err.message);
        return;
      }

      dispatchTestEvent(
        inputEventType.value.trim() || 'custom.event',
        selectSimError.value,
        payload,
        inputEventTopic.value.trim() || 'telex.events.stream'
      );
    });

    btnClearDLQ.addEventListener('click', async () => {
      if (!confirm('Are you sure you want to flush the Dead-Letter Queue?')) return;
      try {
        await fetch('/api/v1/dlq', { method: 'DELETE' });
        fetchDLQ();
      } catch (err) {
        console.error('Failed to clear DLQ:', err);
      }
    });

    btnRefreshDLQ.addEventListener('click', fetchDLQ);
  }

  async function dispatchTestEvent(type, simError, payload, topic = 'telex.events.stream') {
    try {
      const res = await fetch('/api/v1/events/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          topic: topic,
          type: type,
          simulate_error: simError,
          payload: payload
        })
      });
      const data = await res.json();
      fetchRecentEvents();
    } catch (err) {
      alert('Error publishing event: ' + err.message);
    }
  }

  async function fetchStats() {
    try {
      const res = await fetch('/api/v1/stats');
      if (!res.ok) return;
      const stats = await res.json();

      brokerStatus.textContent = stats.broker_type + (stats.broker_connected ? ' (Connected)' : ' (Isolated)');
      brokerDot.className = stats.broker_connected ? 'dot green' : 'dot amber';

      workerSaturation.textContent = `${stats.active_workers}/${stats.total_workers} Active`;
      metricActiveWorkers.textContent = `${stats.active_workers} / ${stats.total_workers}`;
      const workerPct = Math.min(100, Math.round((stats.active_workers / (stats.total_workers || 1)) * 100));
      workerProgress.style.width = `${workerPct}%`;

      metricThroughput.textContent = `${stats.throughput_per_sec.toFixed(1)} events/sec`;

      metricQueueDepth.textContent = stats.queue_depth;
      const queuePct = Math.min(100, Math.round((stats.queue_depth / (stats.queue_capacity || 1000)) * 100));
      queueProgress.style.width = `${queuePct}%`;
      metricQueueCap.textContent = `Capacity: ${stats.queue_capacity} items`;

      metricSuccess.textContent = stats.messages_success.toLocaleString();
      metricRetried.textContent = stats.messages_retried.toLocaleString();
      metricDLQ.textContent = stats.messages_dlq.toLocaleString();
      dlqBadgeCount.textContent = stats.messages_dlq;
      metricTotalConsumed.textContent = `${stats.messages_consumed.toLocaleString()} total ingested`;
    } catch (err) {
      // server starting
    }
  }

  async function fetchRecentEvents() {
    try {
      const res = await fetch('/api/v1/events/recent?limit=40');
      if (!res.ok) return;
      const data = await res.json();
      if (!data.events || data.events.length === 0) return;

      const emptyEl = eventFeed.querySelector('.stream-empty');
      if (emptyEl) emptyEl.remove();

      data.events.forEach(ev => {
        const rowId = `ev-${ev.id}-${ev.status}-${ev.attempt}`;
        if (seenEventIDs.has(rowId)) return;
        seenEventIDs.add(rowId);

        totalRenderedEvents++;
        eventCounter.textContent = `${totalRenderedEvents} events`;

        const row = document.createElement('div');
        row.className = `event-row ${ev.status}`;

        let statusBadge = '<span class="badge badge-success">SUCCEEDED</span>';
        if (ev.status === 'retrying') {
          statusBadge = `<span class="badge badge-amber">RETRYING (${ev.attempt}/${ev.max_retries})</span>`;
        } else if (ev.status === 'dead_letter') {
          statusBadge = '<span class="badge badge-rose">DEAD LETTER</span>';
        } else if (ev.status === 'processing') {
          statusBadge = '<span class="badge" style="background:#0284c7;color:#fff;">PROCESSING</span>';
        }

        const timeStr = ev.last_attempt ? new Date(ev.last_attempt).toLocaleTimeString() : new Date().toLocaleTimeString();

        let errorLine = '';
        if (ev.error_chain && ev.error_chain.length > 0) {
          errorLine = `<div style="font-size:0.75rem;color:#fca5a5;margin-top:2px;">⚠️ ${escapeHtml(ev.error_chain[ev.error_chain.length - 1])}</div>`;
        }

        row.innerHTML = `
          <div class="event-top">
            <div class="event-meta">
              ${statusBadge}
              <span class="badge badge-topic">${escapeHtml(ev.topic)}</span>
              <span class="badge badge-type">${escapeHtml(ev.type)}</span>
              <span style="color:#94a3b8;font-size:0.72rem;">ID: ${ev.id.substring(0, 8)}...</span>
            </div>
            <span class="event-time">${timeStr}</span>
          </div>
          <div class="event-body">${escapeHtml(JSON.stringify(ev.payload || {}))}</div>
          ${errorLine}
        `;

        eventFeed.insertBefore(row, eventFeed.firstChild);

        // Keep DOM compact
        if (eventFeed.children.length > 80) {
          eventFeed.removeChild(eventFeed.lastChild);
        }
      });
    } catch (err) {
      // ignore
    }
  }

  async function fetchDLQ() {
    try {
      const res = await fetch('/api/v1/dlq?limit=50');
      if (!res.ok) return;
      const data = await res.json();

      if (!data.messages || data.messages.length === 0) {
        dlqTableBody.innerHTML = `
          <tr>
            <td colspan="7" class="text-center text-muted">No messages currently in Dead-Letter Queue</td>
          </tr>
        `;
        return;
      }

      dlqTableBody.innerHTML = data.messages.map(msg => {
        const timeStr = new Date(msg.failure_timestamp).toLocaleTimeString();
        const replayedBadge = msg.replayed
          ? '<span class="badge" style="background:rgba(56,189,248,0.2);color:#38bdf8;">REPLAYED</span>'
          : '<span class="badge badge-rose">PENDING</span>';

        return `
          <tr>
            <td><code>${msg.event.id.substring(0, 8)}...</code></td>
            <td>
              <span class="badge badge-topic">${escapeHtml(msg.event.topic)}</span><br>
              <span style="font-size:0.75rem;color:#cbd5e1;">${escapeHtml(msg.event.type)}</span>
            </td>
            <td>
              <span class="text-rose font-mono" style="font-size:0.78rem;">${escapeHtml(msg.reason)}</span>
              <div style="font-size:0.72rem;color:#94a3b8;">${escapeHtml(msg.error_details || '')}</div>
            </td>
            <td>${msg.failed_at_attempt}</td>
            <td style="color:#94a3b8;font-size:0.8rem;">${timeStr}</td>
            <td>${replayedBadge}</td>
            <td>
              <button class="btn btn-primary btn-sm btn-replay" data-id="${msg.event.id}">Replay</button>
            </td>
          </tr>
        `;
      }).join('');

      // Attach replay listeners
      document.querySelectorAll('.btn-replay').forEach(btn => {
        btn.addEventListener('click', async (e) => {
          const id = e.target.getAttribute('data-id');
          btn.disabled = true;
          btn.textContent = 'Replaying...';
          try {
            await fetch('/api/v1/dlq/replay', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ event_id: id })
            });
            fetchDLQ();
            fetchRecentEvents();
          } catch (err) {
            alert('Replay failed: ' + err.message);
            btn.disabled = false;
            btn.textContent = 'Replay';
          }
        });
      });
    } catch (err) {
      console.error('Failed to fetch DLQ:', err);
    }
  }

  function startPolling() {
    fetchStats();
    fetchRecentEvents();
    fetchDLQ();

    setInterval(fetchStats, 1500);
    setInterval(fetchRecentEvents, 1500);
    setInterval(fetchDLQ, 3500);
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }
})();
