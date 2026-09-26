const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

// Configuration
const REPLICATION_FACTOR = 3;
const STORAGE_ROOT = path.join(__dirname, 'storage_cluster');

// In-Memory Cluster Metadata & State
const cluster = {
  nodes: [
    { id: 'node-01', zone: 'us-east-1', isOnline: true },
    { id: 'node-02', zone: 'us-east-1', isOnline: true },
    { id: 'node-03', zone: 'us-east-2', isOnline: true },
    { id: 'node-04', zone: 'us-west-1', isOnline: true },
    { id: 'node-05', zone: 'us-west-2', isOnline: true },
  ],
  catalog: {}, // key -> { hash, size, replicas: [nodeIds], timestamp }
  logs: []
};

// Initialize Storage Directory structure for each node
function initializeClusterStorage() {
  if (!fs.existsSync(STORAGE_ROOT)) {
    fs.mkdirSync(STORAGE_ROOT, { recursive: true });
  }
  cluster.nodes.forEach(node => {
    const nodeDir = path.join(STORAGE_ROOT, node.id);
    if (!fs.existsSync(nodeDir)) {
      fs.mkdirSync(nodeDir, { recursive: true });
    }
  });
  appendLog('System Initialized: 5 physical node directories ready.');
}

function appendLog(msg, type = 'INFO') {
  const entry = {
    id: Date.now() + Math.random().toString(36).substr(2, 4),
    timestamp: new Date().toISOString(),
    type,
    msg
  };
  cluster.logs.unshift(entry);
  if (cluster.logs.length > 50) cluster.logs.pop();
  console.log(`[${entry.type}] ${entry.msg}`);
}

function calculateHash(bufferOrString) {
  return crypto.createHash('sha256').update(bufferOrString).digest('hex');
}

// -------------------------------------------------------------
// REST API ENDPOINTS
// -------------------------------------------------------------

// 1. Get Cluster Health & Topology
app.get('/api/cluster/status', (req, res) => {
  const nodeStatus = cluster.nodes.map(n => {
    const nodeDir = path.join(STORAGE_ROOT, n.id);
    const files = fs.existsSync(nodeDir) ? fs.readdirSync(nodeDir) : [];
    return {
      ...n,
      storedFiles: files.length
    };
  });

  res.json({
    nodes: nodeStatus,
    catalog: cluster.catalog,
    logs: cluster.logs
  });
});

// 2. Ingest / Write Object (Replication across N nodes)
app.post('/api/objects', (req, res) => {
  const { key, payload } = req.body;

  if (!key || payload === undefined) {
    return res.status(400).json({ error: 'Object key and payload are required.' });
  }

  const activeNodes = cluster.nodes.filter(n => n.isOnline);
  if (activeNodes.length < 2) {
    appendLog(`WRITE REJECTED for "${key}": Quorum lost (less than 2 nodes active).`, 'ERROR');
    return res.status(503).json({ error: 'Quorum unavailable. Not enough active nodes to guarantee persistence.' });
  }

  const hash = calculateHash(payload);
  const targetReplicas = Math.min(REPLICATION_FACTOR, activeNodes.length);

  // Distribute across available nodes
  const shuffledNodes = [...activeNodes].sort(() => 0.5 - Math.random());
  const selectedNodes = shuffledNodes.slice(0, targetReplicas);

  const assignedNodeIds = [];
  selectedNodes.forEach(node => {
    const filePath = path.join(STORAGE_ROOT, node.id, encodeURIComponent(key));
    fs.writeFileSync(filePath, payload, 'utf-8');
    assignedNodeIds.push(node.id);
  });

  cluster.catalog[key] = {
    key,
    hash,
    size: Buffer.byteLength(payload, 'utf8'),
    replicas: assignedNodeIds,
    createdAt: new Date().toISOString()
  };

  appendLog(`[WRITE] Object "${key}" replicated across: ${assignedNodeIds.join(', ')} (SHA: ${hash.substring(0, 8)}...)`, 'SUCCESS');
  res.status(201).json({ success: true, metadata: cluster.catalog[key] });
});

// 3. Read Object with Integrity Verification
app.get('/api/objects/:key', (req, res) => {
  const key = req.params.key;
  const meta = cluster.catalog[key];

  if (!meta) {
    return res.status(404).json({ error: 'Object not found in catalog.' });
  }

  let validPayload = null;
  let healthyReplicaFound = false;

  for (const nodeId of meta.replicas) {
    const node = cluster.nodes.find(n => n.id === nodeId);
    if (node && node.isOnline) {
      const filePath = path.join(STORAGE_ROOT, nodeId, encodeURIComponent(key));
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf-8');
        const currentHash = calculateHash(content);

        if (currentHash === meta.hash) {
          validPayload = content;
          healthyReplicaFound = true;
          break; // Consistent copy verified
        } else {
          appendLog(`[DATA CORRUPTION] Bit-rot detected on ${nodeId} for object "${key}"! Checksum mismatch.`, 'WARN');
        }
      }
    }
  }

  if (healthyReplicaFound) {
    res.json({ key, content: validPayload, checksum: meta.hash });
  } else {
    appendLog(`[READ FAILURE] Failed to retrieve "${key}". All copies are offline or corrupted.`, 'ERROR');
    res.status(500).json({ error: 'Object currently unreadable. Available replicas are damaged or offline.' });
  }
});

// 4. Fault Injection: Toggle Node State (Crash or Recover)
app.post('/api/faults/toggle-node', (req, res) => {
  const { nodeId, isOnline } = req.body;
  const node = cluster.nodes.find(n => n.id === nodeId);

  if (!node) return res.status(404).json({ error: 'Node not found.' });

  node.isOnline = Boolean(isOnline);
  appendLog(`[FAULT INJECTION] ${node.id} switched to ${node.isOnline ? 'ONLINE' : 'OFFLINE'}.`, node.isOnline ? 'INFO' : 'WARN');
  res.json({ success: true, node });
});

// 5. Fault Injection: Corrupt a Bit in a Stored Object File
app.post('/api/faults/corrupt-block', (req, res) => {
  const { nodeId, key } = req.body;
  const filePath = path.join(STORAGE_ROOT, nodeId, encodeURIComponent(key));

  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: 'Chunk not located on specified node.' });
  }

  // Inject arbitrary garbage byte
  fs.writeFileSync(filePath, 'CORRUPTED_BIT_ROT_' + Date.now(), 'utf-8');
  appendLog(`[FAULT INJECTION] Bit-rot artificially injected into ${nodeId} for chunk "${key}".`, 'ERROR');
  res.json({ success: true, message: `Corrupted ${key} on ${nodeId}` });
});

// 6. Autonomous Self-Healing & Scrubber Daemon Trigger
app.post('/api/cluster/self-heal', (req, res) => {
  let repairedCount = 0;
  appendLog('[SCRUBBER] Running cluster-wide integrity audit...', 'INFO');

  Object.keys(cluster.catalog).forEach(key => {
    const meta = cluster.catalog[key];
    let validContent = null;

    // Phase 1: Purge corrupted blocks
    meta.replicas = meta.replicas.filter(nodeId => {
      const node = cluster.nodes.find(n => n.id === nodeId);
      if (!node || !node.isOnline) return false;

      const filePath = path.join(STORAGE_ROOT, nodeId, encodeURIComponent(key));
      if (!fs.existsSync(filePath)) return false;

      const content = fs.readFileSync(filePath, 'utf-8');
      if (calculateHash(content) === meta.hash) {
        validContent = content;
        return true;
      } else {
        // Evict corrupted replica
        fs.unlinkSync(filePath);
        appendLog(`[AUTO-HEAL] Evicted corrupted replica from ${nodeId} for key "${key}".`, 'WARN');
        repairedCount++;
        return false;
      }
    });

    // Phase 2: Re-replicate to maintain Replication Factor
    if (validContent) {
      const healthyTargetNodes = cluster.nodes.filter(
        n => n.isOnline && !meta.replicas.includes(n.id)
      );

      while (meta.replicas.length < REPLICATION_FACTOR && healthyTargetNodes.length > 0) {
        const target = healthyTargetNodes.shift();
        const targetPath = path.join(STORAGE_ROOT, target.id, encodeURIComponent(key));
        fs.writeFileSync(targetPath, validContent, 'utf-8');
        meta.replicas.push(target.id);
        appendLog(`[AUTO-HEAL] Re-replicated clean copy of "${key}" onto ${target.id}.`, 'SUCCESS');
        repairedCount++;
      }
    }
  });

  res.json({ success: true, repairedOperations: repairedCount });
});

// Background scrubber timer (Runs every 30 seconds automatically)
setInterval(() => {
  // Silent background health check
  const activeCount = cluster.nodes.filter(n => n.isOnline).length;
  if (activeCount < cluster.nodes.length) {
    appendLog(`[SCRUBBER] Periodic scan detected ${cluster.nodes.length - activeCount} offline node(s).`, 'INFO');
  }
}, 30000);

initializeClusterStorage();

app.get('/', (req, res) => {
  const frontendPath = path.join(__dirname, 'vault_distributed_object_storage_frontend.html');

  if (fs.existsSync(frontendPath)) {
    return res.sendFile(frontendPath);
  }

  res.json({
    message: 'VAULT API is running.',
    endpoints: [
      '/api/cluster/status',
      '/api/objects',
      '/api/objects/:key',
      '/api/faults/toggle-node',
      '/api/faults/corrupt-block',
      '/api/cluster/self-heal'
    ]
  });
});

app.listen(PORT, () => {
  console.log(`Vault Cluster Storage Coordinator active on http://localhost:${PORT}`);
});