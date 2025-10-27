class KafkaVisualDemo {
    constructor() {
        this.socket = io();
        this.topics = new Map();
        this.producers = new Map();
        this.consumers = new Map();
        this.messages = [];
        this.demoMode = false;
        this.autoDemoMode = false;
        this.currentStep = 0;
        this.messageFlowAnimations = [];
        
        this.initializeEventListeners();
        this.initializeSocketListeners();
        this.loadInitialData();
    }

    initializeEventListeners() {
        // Demo control buttons
        document.getElementById('startDemo').addEventListener('click', () => this.startDemo());
        document.getElementById('nextStep').addEventListener('click', () => this.nextStep());
        document.getElementById('autoDemo').addEventListener('click', () => this.startAutoDemo());
        document.getElementById('stopDemo').addEventListener('click', () => this.stopDemo());

        // Modal controls
        document.querySelector('.modal-close').addEventListener('click', () => this.closeModal());
        document.getElementById('messageModal').addEventListener('click', (e) => {
            if (e.target.id === 'messageModal') this.closeModal();
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') this.closeModal();
            if (e.key === ' ' && !this.demoMode) {
                e.preventDefault();
                this.startDemo();
            }
            if (e.key === 'ArrowRight' && this.demoMode) {
                e.preventDefault();
                this.nextStep();
            }
        });
    }

    initializeSocketListeners() {
        this.socket.on('connect', () => {
            console.log('Connected to server');
            this.updateConnectionStatus(true);
        });

        this.socket.on('disconnect', () => {
            console.log('Disconnected from server');
            this.updateConnectionStatus(false);
        });

        this.socket.on('topicCreated', (topic) => {
            this.topics.set(topic.id, topic);
            this.renderTopics();
            this.updateStats();
        });

        this.socket.on('producerCreated', (producer) => {
            this.producers.set(producer.id, producer);
            this.renderProducers();
            this.updateStats();
        });

        this.socket.on('consumerCreated', (consumer) => {
            this.consumers.set(consumer.id, consumer);
            this.renderConsumers();
            this.updateStats();
        });

        this.socket.on('messageProduced', (data) => {
            this.handleMessageProduced(data);
        });

        this.socket.on('messageConsumed', (data) => {
            this.handleMessageConsumed(data);
        });

        this.socket.on('demoStarted', () => {
            this.demoMode = true;
            this.updateDemoControls();
            this.showDemoInstructions();
        });

        this.socket.on('demoStopped', () => {
            this.demoMode = false;
            this.autoDemoMode = false;
            this.updateDemoControls();
            this.hideDemoInstructions();
        });

        this.socket.on('autoDemoStarted', () => {
            this.autoDemoMode = true;
            this.updateDemoControls();
        });

        this.socket.on('demoStep', (data) => {
            this.handleDemoStep(data);
        });
    }

    async loadInitialData() {
        try {
            const [topics, producers, consumers] = await Promise.all([
                fetch('/api/topics').then(r => r.json()),
                fetch('/api/producers').then(r => r.json()),
                fetch('/api/consumers').then(r => r.json())
            ]);

            topics.forEach(topic => this.topics.set(topic.id, topic));
            producers.forEach(producer => this.producers.set(producer.id, producer));
            consumers.forEach(consumer => this.consumers.set(consumer.id, consumer));

            this.renderAll();
            this.updateStats();
        } catch (error) {
            console.error('Error loading initial data:', error);
        }
    }

    renderAll() {
        this.renderProducers();
        this.renderTopics();
        this.renderConsumers();
    }

    renderProducers() {
        const container = document.getElementById('producersContainer');
        container.innerHTML = '';

        this.producers.forEach(producer => {
            const producerCard = this.createProducerCard(producer);
            container.appendChild(producerCard);
        });
    }

    createProducerCard(producer) {
        const card = document.createElement('div');
        card.className = 'producer-card';
        card.dataset.producerId = producer.id;

        card.innerHTML = `
            <div class="producer-header">
                <div class="producer-name">${producer.name}</div>
                <div class="producer-type ${producer.type}">${producer.type}</div>
            </div>
            <div class="producer-topic">Topic: ${producer.topic}</div>
            <div class="producer-stats">
                <span>Messages: ${producer.messageCount}</span>
                <span>${producer.isActive ? 'Active' : 'Inactive'}</span>
            </div>
        `;

        // Add click handler for manual message sending
        card.addEventListener('click', () => {
            this.sendTestMessage(producer);
        });

        return card;
    }

    renderTopics() {
        const container = document.getElementById('topicsContainer');
        container.innerHTML = '';

        this.topics.forEach(topic => {
            const topicCard = this.createTopicCard(topic);
            container.appendChild(topicCard);
        });
    }

    createTopicCard(topic) {
        const card = document.createElement('div');
        card.className = 'topic-card';
        card.dataset.topicId = topic.id;

        const partitions = Array.from({ length: topic.partitions }, (_, i) => {
            const count = topic.partitionMessages ? topic.partitionMessages[i] || 0 : 0;
            return `
                <div class="partition" data-partition="${i}">
                    <div class="partition-number">P${i}</div>
                    <div class="partition-count">${count}</div>
                </div>
            `;
        }).join('');

        card.innerHTML = `
            <div class="topic-header">
                <div class="topic-name">${topic.name}</div>
                <div class="topic-partitions">${topic.partitions} partitions</div>
            </div>
            <div class="partitions-container">
                ${partitions}
            </div>
        `;

        return card;
    }

    renderConsumers() {
        const container = document.getElementById('consumersContainer');
        container.innerHTML = '';

        this.consumers.forEach(consumer => {
            const consumerCard = this.createConsumerCard(consumer);
            container.appendChild(consumerCard);
        });
    }

    createConsumerCard(consumer) {
        const card = document.createElement('div');
        card.className = 'consumer-card';
        card.dataset.consumerId = consumer.id;

        const topicsList = consumer.topics.join(', ');

        card.innerHTML = `
            <div class="consumer-header">
                <div class="consumer-name">${consumer.name}</div>
                <div class="consumer-type">${consumer.type}</div>
            </div>
            <div class="consumer-topics">Topics: ${topicsList}</div>
            <div class="consumer-stats">
                <span>Processed: ${consumer.processedMessages}</span>
                <span>${consumer.isActive ? 'Active' : 'Inactive'}</span>
            </div>
        `;

        return card;
    }

    handleMessageProduced(data) {
        const { message, producer, topic } = data;
        
        // Update producer card
        const producerCard = document.querySelector(`[data-producer-id="${producer.id}"]`);
        if (producerCard) {
            producerCard.classList.add('active');
            producerCard.querySelector('.producer-stats span').textContent = `Messages: ${producer.messageCount}`;
            setTimeout(() => producerCard.classList.remove('active'), 2000);
        }

        // Update topic partitions
        const topicCard = document.querySelector(`[data-topic-id="${topic.id}"]`);
        if (topicCard) {
            const partition = topicCard.querySelector(`[data-partition="${message.partition}"]`);
            if (partition) {
                partition.classList.add('active');
                const countElement = partition.querySelector('.partition-count');
                countElement.textContent = topic.partitionMessages[message.partition];
                setTimeout(() => partition.classList.remove('active'), 2000);
            }
        }

        // Add message to history
        this.addMessageToHistory(message, 'produced');
        
        // Show message flow animation
        this.showMessageFlowAnimation(message, 'producer', 'topic');
        
        // Update stats
        this.updateStats();
    }

    handleMessageConsumed(data) {
        const { message, consumer } = data;
        
        // Update consumer card
        const consumerCard = document.querySelector(`[data-consumer-id="${consumer.id}"]`);
        if (consumerCard) {
            consumerCard.classList.add('active');
            consumerCard.querySelector('.consumer-stats span').textContent = `Processed: ${consumer.processedMessages}`;
            setTimeout(() => consumerCard.classList.remove('active'), 2000);
        }

        // Show message flow animation
        this.showMessageFlowAnimation(message, 'topic', 'consumer');
        
        // Update stats
        this.updateStats();
    }

    showMessageFlowAnimation(message, from, to) {
        const container = document.getElementById('messageFlowContainer');
        container.classList.add('active');

        const animationElement = document.createElement('div');
        animationElement.className = 'message-flow-animation';
        animationElement.style.cssText = `
            position: absolute;
            top: ${Math.random() * 150 + 25}px;
            left: -100px;
            width: 80px;
            height: 40px;
            background: linear-gradient(135deg, #667eea, #764ba2);
            border-radius: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 0.8rem;
            font-weight: 500;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
            z-index: 10;
        `;
        animationElement.textContent = message.id.substring(0, 8);

        container.appendChild(animationElement);

        // Remove animation element after completion
        setTimeout(() => {
            if (animationElement.parentNode) {
                animationElement.parentNode.removeChild(animationElement);
            }
        }, 3000);

        // Remove active class after animation
        setTimeout(() => {
            container.classList.remove('active');
        }, 3000);
    }

    addMessageToHistory(message, type) {
        const container = document.getElementById('messageHistoryContainer');
        
        const messageElement = document.createElement('div');
        messageElement.className = `message-item ${type === 'produced' ? 'new' : ''}`;
        messageElement.innerHTML = `
            <div class="message-header">
                <div class="message-id">${message.id.substring(0, 12)}...</div>
                <div class="message-timestamp">${new Date(message.timestamp).toLocaleTimeString()}</div>
            </div>
            <div class="message-content">${JSON.stringify(message.data, null, 2)}</div>
            <div class="message-meta">
                <span>Producer: ${message.producerId}</span>
                <span>Topic: ${message.topic}</span>
                <span>Partition: ${message.partition}</span>
                <span>Offset: ${message.offset}</span>
            </div>
        `;

        // Add click handler to show message details
        messageElement.addEventListener('click', () => {
            this.showMessageDetails(message);
        });

        // Insert at the beginning
        container.insertBefore(messageElement, container.firstChild);

        // Keep only last 50 messages
        while (container.children.length > 50) {
            container.removeChild(container.lastChild);
        }

        // Remove 'new' class after animation
        setTimeout(() => {
            messageElement.classList.remove('new');
        }, 500);
    }

    showMessageDetails(message) {
        const modal = document.getElementById('messageModal');
        const detailsElement = document.getElementById('messageDetails');
        
        detailsElement.textContent = JSON.stringify(message, null, 2);
        modal.classList.remove('hidden');
    }

    closeModal() {
        const modal = document.getElementById('messageModal');
        modal.classList.add('hidden');
    }

    updateStats() {
        document.getElementById('totalTopics').textContent = this.topics.size;
        document.getElementById('totalProducers').textContent = this.producers.size;
        document.getElementById('totalConsumers').textContent = this.consumers.size;
        document.getElementById('totalMessages').textContent = this.messages.length;
    }

    updateConnectionStatus(connected) {
        const statusIndicator = document.querySelector('.connection-status');
        if (statusIndicator) {
            statusIndicator.className = `connection-status ${connected ? 'connected' : 'disconnected'}`;
        }
    }

    updateDemoControls() {
        const startBtn = document.getElementById('startDemo');
        const nextBtn = document.getElementById('nextStep');
        const autoBtn = document.getElementById('autoDemo');
        const stopBtn = document.getElementById('stopDemo');

        if (this.demoMode || this.autoDemoMode) {
            startBtn.disabled = true;
            nextBtn.disabled = !this.demoMode;
            autoBtn.disabled = true;
            stopBtn.disabled = false;
        } else {
            startBtn.disabled = false;
            nextBtn.disabled = true;
            autoBtn.disabled = false;
            stopBtn.disabled = true;
        }
    }

    showDemoInstructions() {
        const instructions = document.getElementById('demoInstructions');
        instructions.classList.remove('hidden');
    }

    hideDemoInstructions() {
        const instructions = document.getElementById('demoInstructions');
        instructions.classList.add('hidden');
    }

    handleDemoStep(data) {
        const { step, data: stepData } = data;
        this.currentStep = step;
        
        // Update instruction content
        document.getElementById('instructionTitle').textContent = stepData.message;
        document.getElementById('instructionText').textContent = stepData.message;
        
        // Update progress
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');
        const progress = ((step + 1) / this.demoSteps.length) * 100;
        
        progressFill.style.width = `${progress}%`;
        progressText.textContent = `Step ${step + 1} of ${this.demoSteps.length}`;
        
        // Highlight relevant components
        this.highlightComponents(stepData.highlight);
    }

    highlightComponents(highlight) {
        // Remove existing highlights
        document.querySelectorAll('.highlight').forEach(el => {
            el.classList.remove('highlight');
        });

        if (highlight === 'all') {
            document.querySelectorAll('.producer-card, .topic-card, .consumer-card').forEach(el => {
                el.classList.add('highlight');
            });
        } else if (highlight === 'producers') {
            document.querySelectorAll('.producer-card').forEach(el => {
                el.classList.add('highlight');
            });
        } else if (highlight === 'topics') {
            document.querySelectorAll('.topic-card').forEach(el => {
                el.classList.add('highlight');
            });
        } else if (highlight === 'consumers') {
            document.querySelectorAll('.consumer-card').forEach(el => {
                el.classList.add('highlight');
            });
        }
    }

    async startDemo() {
        try {
            await fetch('/api/demo/start', { method: 'POST' });
        } catch (error) {
            console.error('Error starting demo:', error);
        }
    }

    async nextStep() {
        try {
            await fetch('/api/demo/step', { method: 'POST' });
        } catch (error) {
            console.error('Error advancing demo step:', error);
        }
    }

    async startAutoDemo() {
        try {
            await fetch('/api/demo/auto', { method: 'POST' });
        } catch (error) {
            console.error('Error starting auto demo:', error);
        }
    }

    async stopDemo() {
        try {
            await fetch('/api/demo/stop', { method: 'POST' });
        } catch (error) {
            console.error('Error stopping demo:', error);
        }
    }

    async sendTestMessage(producer) {
        const sampleMessages = {
            'user-events': [
                { type: 'login', userId: 'user123', sessionId: 'sess456', timestamp: Date.now() },
                { type: 'page_view', userId: 'user123', page: '/products', duration: 5000 },
                { type: 'click', userId: 'user123', element: 'add-to-cart', productId: 'prod789' }
            ],
            'order-events': [
                { type: 'order_created', orderId: 'order456', userId: 'user123', amount: 99.99 },
                { type: 'payment_processed', orderId: 'order456', method: 'credit_card', amount: 99.99 },
                { type: 'order_shipped', orderId: 'order456', trackingNumber: 'TRK789', carrier: 'UPS' }
            ],
            'analytics-data': [
                { type: 'page_view', page: '/home', duration: 5000, userId: 'user123' },
                { type: 'conversion', funnel: 'checkout', step: 1, userId: 'user123' },
                { type: 'bounce', page: '/products', timeOnPage: 2000, userId: 'user123' }
            ],
            'iot-sensors': [
                { sensorId: 'temp-001', value: 23.5, unit: 'celsius', location: 'room1', timestamp: Date.now() },
                { sensorId: 'humidity-001', value: 65, unit: 'percent', location: 'room1', timestamp: Date.now() },
                { sensorId: 'motion-001', detected: true, location: 'entrance', timestamp: Date.now() }
            ],
            'payment-events': [
                { type: 'payment_initiated', orderId: 'order456', amount: 99.99, method: 'credit_card' },
                { type: 'payment_authorized', orderId: 'order456', transactionId: 'txn789' },
                { type: 'payment_completed', orderId: 'order456', status: 'success' }
            ],
            'inventory-updates': [
                { productId: 'prod789', action: 'stock_reduced', quantity: 1, remaining: 45 },
                { productId: 'prod789', action: 'restock', quantity: 100, total: 144 },
                { productId: 'prod123', action: 'low_stock', quantity: 5, threshold: 10 }
            ]
        };

        const messages = sampleMessages[producer.topic] || [];
        const randomMessage = messages[Math.floor(Math.random() * messages.length)];

        try {
            await fetch(`/api/producer/${producer.id}/send`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: randomMessage,
                    topic: producer.topic
                })
            });
        } catch (error) {
            console.error('Error sending message:', error);
        }
    }
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.kafkaDemo = new KafkaVisualDemo();
    
    // Add some helpful console messages
    console.log('🚀 Kafka Visual Demo loaded!');
    console.log('📖 Use keyboard shortcuts:');
    console.log('   Space: Start demo');
    console.log('   Right Arrow: Next step (during demo)');
    console.log('   Escape: Close modal');
    console.log('🎯 Click on producer cards to send test messages');
    console.log('📊 Click on message history items to view details');
});
