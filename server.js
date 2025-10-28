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

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({ 
    success: false, 
    error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? err.message : 'Something went wrong'
  });
});

// API Routes with error handling
app.get('/api/topics', (req, res) => {
  try {
    const topics = kafkaSimulator.getTopics();
    res.json({ success: true, data: topics });
  } catch (error) {
    console.error('Error fetching topics:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch topics' });
  }
});

app.get('/api/producers', (req, res) => {
  try {
    const producers = kafkaSimulator.getProducers();
    res.json({ success: true, data: producers });
  } catch (error) {
    console.error('Error fetching producers:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch producers' });
  }
});

app.get('/api/consumers', (req, res) => {
  try {
    const consumers = kafkaSimulator.getConsumers();
    res.json({ success: true, data: consumers });
  } catch (error) {
    console.error('Error fetching consumers:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch consumers' });
  }
});

app.get('/api/consumer-groups', (req, res) => {
  try {
    const consumerGroups = kafkaSimulator.getConsumerGroups();
    res.json({ success: true, data: consumerGroups });
  } catch (error) {
    console.error('Error fetching consumer groups:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch consumer groups' });
  }
});

app.get('/api/dead-letter-queues', (req, res) => {
  try {
    const deadLetterQueues = kafkaSimulator.getDeadLetterQueues();
    res.json({ success: true, data: deadLetterQueues });
  } catch (error) {
    console.error('Error fetching dead letter queues:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch dead letter queues' });
  }
});

app.get('/api/messages/:topic', (req, res) => {
  try {
    const { topic } = req.params;
    
    // Validate topic parameter
    if (!topic || typeof topic !== 'string') {
      return res.status(400).json({ success: false, error: 'Invalid topic parameter' });
    }
    
    const messages = kafkaSimulator.getMessages(topic);
    res.json({ success: true, data: messages });
  } catch (error) {
    console.error('Error fetching messages:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch messages' });
  }
});

app.post('/api/producer/:id/send', (req, res) => {
  try {
    const { id } = req.params;
    const { message, topic } = req.body;
    
    // Validate parameters
    if (!id || typeof id !== 'string') {
      return res.status(400).json({ success: false, error: 'Invalid producer ID' });
    }
    
    if (!message || typeof message !== 'object') {
      return res.status(400).json({ success: false, error: 'Invalid message data' });
    }
    
    if (!topic || typeof topic !== 'string') {
      return res.status(400).json({ success: false, error: 'Invalid topic' });
    }
    
    // Check if producer exists
    const producers = kafkaSimulator.getProducers();
    const producer = producers.find(p => p.id === id);
    if (!producer) {
      return res.status(404).json({ success: false, error: 'Producer not found' });
    }
    
    // Check if topic exists
    const topics = kafkaSimulator.getTopics();
    const topicExists = topics.find(t => t.id === topic);
    if (!topicExists) {
      return res.status(404).json({ success: false, error: 'Topic not found' });
    }
    
    kafkaSimulator.sendMessage(id, topic, message);
    res.json({ success: true, message: 'Message sent successfully' });
  } catch (error) {
    console.error('Error sending message:', error);
    res.status(500).json({ success: false, error: 'Failed to send message' });
  }
});

app.post('/api/demo/start', (req, res) => {
  try {
    kafkaSimulator.startDemo();
    res.json({ success: true, message: 'Demo started successfully' });
  } catch (error) {
    console.error('Error starting demo:', error);
    res.status(500).json({ success: false, error: 'Failed to start demo' });
  }
});

app.post('/api/demo/stop', (req, res) => {
  try {
    kafkaSimulator.stopDemo();
    res.json({ success: true, message: 'Demo stopped successfully' });
  } catch (error) {
    console.error('Error stopping demo:', error);
    res.status(500).json({ success: false, error: 'Failed to stop demo' });
  }
});

app.post('/api/demo/step', (req, res) => {
  try {
    kafkaSimulator.nextStep();
    res.json({ success: true, message: 'Demo step advanced successfully' });
  } catch (error) {
    console.error('Error advancing demo step:', error);
    res.status(500).json({ success: false, error: 'Failed to advance demo step' });
  }
});

app.post('/api/demo/auto', (req, res) => {
  try {
    kafkaSimulator.startAutoDemo();
    res.json({ success: true, message: 'Auto demo started successfully' });
  } catch (error) {
    console.error('Error starting auto demo:', error);
    res.status(500).json({ success: false, error: 'Failed to start auto demo' });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  try {
    const stats = kafkaSimulator.getStats();
    res.json({ 
      success: true, 
      status: 'healthy',
      timestamp: new Date().toISOString(),
      stats: stats
    });
  } catch (error) {
    console.error('Health check failed:', error);
    res.status(500).json({ 
      success: false, 
      status: 'unhealthy',
      error: 'Health check failed'
    });
  }
});

// Persistence and replay endpoints
app.post('/api/replay/:topic', (req, res) => {
  try {
    const { topic } = req.params;
    const { fromOffset = 0, toOffset = null } = req.body;
    
    if (!topic || typeof topic !== 'string') {
      return res.status(400).json({ success: false, error: 'Invalid topic parameter' });
    }
    
    const replayCount = kafkaSimulator.replayMessages(topic, fromOffset, toOffset);
    res.json({ 
      success: true, 
      message: `Replaying ${replayCount} messages from topic ${topic}`,
      replayCount: replayCount
    });
  } catch (error) {
    console.error('Error replaying messages:', error);
    res.status(500).json({ success: false, error: 'Failed to replay messages' });
  }
});

app.post('/api/persistence/toggle', (req, res) => {
  try {
    const enabled = kafkaSimulator.togglePersistence();
    res.json({ 
      success: true, 
      message: `Persistence ${enabled ? 'enabled' : 'disabled'}`,
      persistenceEnabled: enabled
    });
  } catch (error) {
    console.error('Error toggling persistence:', error);
    res.status(500).json({ success: false, error: 'Failed to toggle persistence' });
  }
});

app.post('/api/data/clear', (req, res) => {
  try {
    kafkaSimulator.clearAllData();
    res.json({ 
      success: true, 
      message: 'All data cleared successfully'
    });
  } catch (error) {
    console.error('Error clearing data:', error);
    res.status(500).json({ success: false, error: 'Failed to clear data' });
  }
});

app.post('/api/data/persist', (req, res) => {
  try {
    kafkaSimulator.persistData();
    res.json({ 
      success: true, 
      message: 'Data persisted successfully'
    });
  } catch (error) {
    console.error('Error persisting data:', error);
    res.status(500).json({ success: false, error: 'Failed to persist data' });
  }
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
