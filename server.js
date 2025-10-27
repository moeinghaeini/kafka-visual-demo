const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const path = require('path');
const { KafkaSimulator } = require('./kafka-simulator');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Initialize Kafka simulator
const kafkaSimulator = new KafkaSimulator(io);

// API Routes
app.get('/api/topics', (req, res) => {
  res.json(kafkaSimulator.getTopics());
});

app.get('/api/producers', (req, res) => {
  res.json(kafkaSimulator.getProducers());
});

app.get('/api/consumers', (req, res) => {
  res.json(kafkaSimulator.getConsumers());
});

app.get('/api/messages/:topic', (req, res) => {
  const { topic } = req.params;
  const messages = kafkaSimulator.getMessages(topic);
  res.json(messages);
});

app.post('/api/producer/:id/send', (req, res) => {
  const { id } = req.params;
  const { message, topic } = req.body;
  kafkaSimulator.sendMessage(id, topic, message);
  res.json({ success: true });
});

app.post('/api/demo/start', (req, res) => {
  kafkaSimulator.startDemo();
  res.json({ success: true });
});

app.post('/api/demo/stop', (req, res) => {
  kafkaSimulator.stopDemo();
  res.json({ success: true });
});

app.post('/api/demo/step', (req, res) => {
  kafkaSimulator.nextStep();
  res.json({ success: true });
});

app.post('/api/demo/auto', (req, res) => {
  kafkaSimulator.startAutoDemo();
  res.json({ success: true });
});

// Socket.io connection handling
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  
  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Kafka Visual Demo server running on port ${PORT}`);
  console.log(`Open http://localhost:${PORT} to view the demo`);
});
