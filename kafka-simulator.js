const { v4: uuidv4 } = require('uuid');

class KafkaSimulator {
  constructor(io) {
    this.io = io;
    this.topics = new Map();
    this.producers = new Map();
    this.consumers = new Map();
    this.messages = new Map();
    this.demoMode = false;
    this.autoDemoMode = false;
    this.currentStep = 0;
    this.demoSteps = [];
    this.messageHistory = [];
    
    this.initializeDemo();
  }

  initializeDemo() {
    // Create topics with different partition counts
    this.createTopic('user-events', 3);
    this.createTopic('order-events', 2);
    this.createTopic('analytics-data', 4);
    this.createTopic('iot-sensors', 5);
    this.createTopic('payment-events', 2);
    this.createTopic('inventory-updates', 3);

    // Create diverse producers
    this.createProducer('web-app', 'Web Application', 'user-events', 'web');
    this.createProducer('mobile-app', 'Mobile App', 'user-events', 'mobile');
    this.createProducer('ecommerce-backend', 'E-commerce Backend', 'order-events', 'backend');
    this.createProducer('analytics-service', 'Analytics Service', 'analytics-data', 'service');
    this.createProducer('iot-gateway', 'IoT Gateway', 'iot-sensors', 'iot');
    this.createProducer('temp-sensor', 'Temperature Sensor', 'iot-sensors', 'sensor');
    this.createProducer('humidity-sensor', 'Humidity Sensor', 'iot-sensors', 'sensor');
    this.createProducer('motion-sensor', 'Motion Sensor', 'iot-sensors', 'sensor');
    this.createProducer('payment-gateway', 'Payment Gateway', 'payment-events', 'gateway');
    this.createProducer('inventory-system', 'Inventory System', 'inventory-updates', 'system');

    // Create consumers with different processing patterns
    this.createConsumer('user-processor', 'User Event Processor', ['user-events'], 'processor');
    this.createConsumer('order-processor', 'Order Processor', ['order-events'], 'processor');
    this.createConsumer('analytics-processor', 'Analytics Processor', ['analytics-data'], 'processor');
    this.createConsumer('iot-processor', 'IoT Data Processor', ['iot-sensors'], 'processor');
    this.createConsumer('notification-service', 'Notification Service', ['user-events', 'order-events'], 'service');
    this.createConsumer('payment-processor', 'Payment Processor', ['payment-events'], 'processor');
    this.createConsumer('inventory-processor', 'Inventory Processor', ['inventory-updates'], 'processor');
    this.createConsumer('dashboard-consumer', 'Real-time Dashboard', ['user-events', 'analytics-data', 'iot-sensors'], 'dashboard');

    // Initialize demo steps
    this.demoSteps = [
      { 
        type: 'intro', 
        message: 'Welcome to Kafka Visual Demo! Let\'s explore how Apache Kafka works with multiple producers and consumers.',
        highlight: 'all'
      },
      { 
        type: 'topics', 
        message: 'Topics are categories for organizing messages. Each topic can have multiple partitions for parallel processing.',
        highlight: 'topics'
      },
      { 
        type: 'producers', 
        message: 'Producers send messages to topics. Different applications (web, mobile, IoT, etc.) can send different types of data.',
        highlight: 'producers'
      },
      { 
        type: 'partitions', 
        message: 'Messages are distributed across partitions. Each partition maintains message order and enables parallel processing.',
        highlight: 'partitions'
      },
      { 
        type: 'consumers', 
        message: 'Consumers read messages from topics. Multiple consumers can process messages in parallel for scalability.',
        highlight: 'consumers'
      },
      { 
        type: 'flow', 
        message: 'Watch how messages flow from producers through topics and partitions to consumers in real-time!',
        highlight: 'flow'
      }
    ];
  }

  createTopic(name, partitions) {
    const topic = {
      id: name,
      name: name,
      partitions: partitions,
      createdAt: new Date(),
      messageCount: 0,
      partitionMessages: new Array(partitions).fill(0)
    };
    this.topics.set(name, topic);
    this.messages.set(name, []);
    this.io.emit('topicCreated', topic);
  }

  createProducer(id, name, topic, type) {
    const producer = {
      id: id,
      name: name,
      topic: topic,
      type: type,
      messageCount: 0,
      isActive: false,
      createdAt: new Date(),
      lastMessageTime: null
    };
    this.producers.set(id, producer);
    this.io.emit('producerCreated', producer);
  }

  createConsumer(id, name, topics, type) {
    const consumer = {
      id: id,
      name: name,
      topics: topics,
      type: type,
      processedMessages: 0,
      isActive: false,
      createdAt: new Date(),
      lastProcessedTime: null
    };
    this.consumers.set(id, consumer);
    this.io.emit('consumerCreated', consumer);
  }

  sendMessage(producerId, topicName, messageData) {
    const producer = this.producers.get(producerId);
    if (!producer) return;

    const topic = this.topics.get(topicName);
    if (!topic) return;

    // Determine partition (can be random or based on key)
    const partition = messageData.key ? 
      Math.abs(messageData.key.split('').reduce((a, b) => a + b.charCodeAt(0), 0)) % topic.partitions :
      Math.floor(Math.random() * topic.partitions);

    const message = {
      id: uuidv4(),
      producerId: producerId,
      topic: topicName,
      partition: partition,
      data: messageData,
      timestamp: new Date(),
      offset: topic.messageCount,
      key: messageData.key || null
    };

    // Add message to topic
    const topicMessages = this.messages.get(topicName) || [];
    topicMessages.push(message);
    this.messages.set(topicName, topicMessages);

    // Update counters
    producer.messageCount++;
    producer.lastMessageTime = new Date();
    topic.messageCount++;
    topic.partitionMessages[partition]++;

    // Add to message history
    this.messageHistory.push(message);
    if (this.messageHistory.length > 1000) {
      this.messageHistory = this.messageHistory.slice(-1000);
    }

    // Emit events
    this.io.emit('messageProduced', {
      message: message,
      producer: producer,
      topic: topic
    });

    // Simulate consumer processing with different delays
    setTimeout(() => {
      this.processMessage(message);
    }, Math.random() * 1500 + 300);
  }

  processMessage(message) {
    const consumers = Array.from(this.consumers.values())
      .filter(consumer => consumer.topics.includes(message.topic));

    consumers.forEach(consumer => {
      consumer.processedMessages++;
      consumer.lastProcessedTime = new Date();
      this.io.emit('messageConsumed', {
        message: message,
        consumer: consumer
      });
    });
  }

  startDemo() {
    this.demoMode = true;
    this.currentStep = 0;
    this.io.emit('demoStarted');
    this.nextStep();
  }

  stopDemo() {
    this.demoMode = false;
    this.autoDemoMode = false;
    this.currentStep = 0;
    this.io.emit('demoStopped');
  }

  nextStep() {
    if (this.currentStep < this.demoSteps.length) {
      const step = this.demoSteps[this.currentStep];
      this.io.emit('demoStep', {
        step: this.currentStep,
        data: step
      });
      this.currentStep++;
    }
  }

  startAutoDemo() {
    this.autoDemoMode = true;
    this.io.emit('autoDemoStarted');
    
    const sampleMessages = {
      'user-events': [
        { type: 'login', userId: 'user123', sessionId: 'sess456', timestamp: Date.now(), key: 'user123' },
        { type: 'page_view', userId: 'user123', page: '/products', duration: 5000, key: 'user123' },
        { type: 'click', userId: 'user123', element: 'add-to-cart', productId: 'prod789', key: 'user123' },
        { type: 'logout', userId: 'user123', sessionId: 'sess456', timestamp: Date.now(), key: 'user123' }
      ],
      'order-events': [
        { type: 'order_created', orderId: 'order456', userId: 'user123', amount: 99.99, key: 'order456' },
        { type: 'payment_processed', orderId: 'order456', method: 'credit_card', amount: 99.99, key: 'order456' },
        { type: 'order_shipped', orderId: 'order456', trackingNumber: 'TRK789', carrier: 'UPS', key: 'order456' },
        { type: 'order_delivered', orderId: 'order456', deliveryTime: Date.now(), key: 'order456' }
      ],
      'analytics-data': [
        { type: 'page_view', page: '/home', duration: 5000, userId: 'user123', key: 'analytics' },
        { type: 'conversion', funnel: 'checkout', step: 1, userId: 'user123', key: 'analytics' },
        { type: 'bounce', page: '/products', timeOnPage: 2000, userId: 'user123', key: 'analytics' },
        { type: 'engagement', action: 'scroll', depth: 80, userId: 'user123', key: 'analytics' }
      ],
      'iot-sensors': [
        { sensorId: 'temp-001', value: 23.5, unit: 'celsius', location: 'room1', timestamp: Date.now(), key: 'temp-001' },
        { sensorId: 'humidity-001', value: 65, unit: 'percent', location: 'room1', timestamp: Date.now(), key: 'humidity-001' },
        { sensorId: 'motion-001', detected: true, location: 'entrance', timestamp: Date.now(), key: 'motion-001' },
        { sensorId: 'pressure-001', value: 1013.25, unit: 'hPa', location: 'outdoor', timestamp: Date.now(), key: 'pressure-001' }
      ],
      'payment-events': [
        { type: 'payment_initiated', orderId: 'order456', amount: 99.99, method: 'credit_card', key: 'order456' },
        { type: 'payment_authorized', orderId: 'order456', transactionId: 'txn789', key: 'order456' },
        { type: 'payment_completed', orderId: 'order456', status: 'success', key: 'order456' }
      ],
      'inventory-updates': [
        { productId: 'prod789', action: 'stock_reduced', quantity: 1, remaining: 45, key: 'prod789' },
        { productId: 'prod789', action: 'restock', quantity: 100, total: 144, key: 'prod789' },
        { productId: 'prod123', action: 'low_stock', quantity: 5, threshold: 10, key: 'prod123' }
      ]
    };

    let messageIndex = 0;
    const sendNextMessage = () => {
      if (!this.autoDemoMode) return;

      const topics = Object.keys(sampleMessages);
      const topic = topics[messageIndex % topics.length];
      const messages = sampleMessages[topic];
      const message = messages[Math.floor(Math.random() * messages.length)];

      const producers = Array.from(this.producers.values())
        .filter(p => p.topic === topic);
      
      if (producers.length > 0) {
        const producer = producers[Math.floor(Math.random() * producers.length)];
        this.sendMessage(producer.id, topic, message);
      }

      messageIndex++;
      setTimeout(sendNextMessage, Math.random() * 2000 + 1000);
    };

    sendNextMessage();
  }

  getTopics() {
    return Array.from(this.topics.values());
  }

  getProducers() {
    return Array.from(this.producers.values());
  }

  getConsumers() {
    return Array.from(this.consumers.values());
  }

  getMessages(topic) {
    return this.messages.get(topic) || [];
  }

  getMessageHistory() {
    return this.messageHistory.slice(-100);
  }

  getStats() {
    return {
      totalTopics: this.topics.size,
      totalProducers: this.producers.size,
      totalConsumers: this.consumers.size,
      totalMessages: this.messageHistory.length,
      demoMode: this.demoMode,
      autoDemoMode: this.autoDemoMode
    };
  }
}

module.exports = { KafkaSimulator };
