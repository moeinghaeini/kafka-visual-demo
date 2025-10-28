class KafkaVisualDemo {
    constructor() {
        this.socket = io();
        this.topics = new Map();
        this.producers = new Map();
        this.consumers = new Map();
        this.consumerGroups = new Map();
        this.messages = [];
        this.demoMode = false;
        this.autoDemoMode = false;
        this.currentStep = 0;
        this.messageFlowAnimations = [];
        
        // Chart data
        this.throughputData = [];
        this.latencyData = [];
        this.charts = {};
        
        // Message filtering
        this.filteredMessages = [];
        this.currentFilters = {
            search: '',
            topic: '',
            type: ''
        };
        
        // Theme management
        this.isDarkTheme = localStorage.getItem('darkTheme') === 'true';
        
        this.initializeEventListeners();
        this.initializeSocketListeners();
        this.initializeCharts();
        this.initializeTheme();
        this.loadInitialData();
    }

    initializeEventListeners() {
        // Demo control buttons
        document.getElementById('startDemo').addEventListener('click', () => this.startDemo());
        document.getElementById('nextStep').addEventListener('click', () => this.nextStep());
        document.getElementById('autoDemo').addEventListener('click', () => this.startAutoDemo());
        document.getElementById('stopDemo').addEventListener('click', () => this.stopDemo());

        // Advanced controls
        document.getElementById('togglePersistence').addEventListener('click', () => this.togglePersistence());
        document.getElementById('persistData').addEventListener('click', () => this.persistData());
        document.getElementById('clearData').addEventListener('click', () => this.clearData());
        document.getElementById('replayMessages').addEventListener('click', () => this.replayMessages());

        // Message filtering and search
        document.getElementById('messageSearch').addEventListener('input', () => this.filterMessages());
        document.getElementById('messageFilter').addEventListener('change', () => this.filterMessages());
        document.getElementById('messageTypeFilter').addEventListener('change', () => this.filterMessages());
        document.getElementById('clearFilters').addEventListener('click', () => this.clearMessageFilters());

        // Theme toggle
        document.getElementById('themeToggle').addEventListener('click', () => this.toggleTheme());

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

    initializeTheme() {
        if (this.isDarkTheme) {
            document.body.classList.add('dark-theme');
            this.updateThemeButton(true);
        } else {
            document.body.classList.remove('dark-theme');
            this.updateThemeButton(false);
        }
    }

    toggleTheme() {
        this.isDarkTheme = !this.isDarkTheme;
        localStorage.setItem('darkTheme', this.isDarkTheme);
        
        if (this.isDarkTheme) {
            document.body.classList.add('dark-theme');
        } else {
            document.body.classList.remove('dark-theme');
        }
        
        this.updateThemeButton(this.isDarkTheme);
    }

    updateThemeButton(isDark) {
        const themeBtn = document.getElementById('themeToggle');
        const icon = themeBtn.querySelector('i');
        
        if (isDark) {
            icon.className = 'fas fa-sun';
            themeBtn.innerHTML = '<i class="fas fa-sun"></i> Light Mode';
        } else {
            icon.className = 'fas fa-moon';
            themeBtn.innerHTML = '<i class="fas fa-moon"></i> Dark Mode';
        }
    }

    initializeCharts() {
        // Initialize throughput chart
        const throughputCtx = document.getElementById('throughputChart').getContext('2d');
        this.charts.throughput = new Chart(throughputCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Messages/sec',
                    data: [],
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Messages per Second'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Time'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                },
                animation: {
                    duration: 750
                }
            }
        });

        // Initialize latency chart
        const latencyCtx = document.getElementById('latencyChart').getContext('2d');
        this.charts.latency = new Chart(latencyCtx, {
            type: 'bar',
            data: {
                labels: ['< 100ms', '100-500ms', '500ms-1s', '1-2s', '> 2s'],
                datasets: [{
                    label: 'Message Count',
                    data: [0, 0, 0, 0, 0],
                    backgroundColor: [
                        'rgba(72, 187, 120, 0.8)',
                        'rgba(102, 126, 234, 0.8)',
                        'rgba(237, 137, 54, 0.8)',
                        'rgba(245, 101, 101, 0.8)',
                        'rgba(128, 90, 213, 0.8)'
                    ],
                    borderColor: [
                        '#48bb78',
                        '#667eea',
                        '#ed8936',
                        '#f56565',
                        '#805ad5'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Message Count'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Latency Range'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                },
                animation: {
                    duration: 750
                }
            }
        });

        // Start updating charts every second
        setInterval(() => this.updateCharts(), 1000);
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

        this.socket.on('consumerGroupCreated', (group) => {
            this.consumerGroups.set(group.id, group);
            this.renderConsumerGroups();
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
        this.showLoadingOverlay('Loading Kafka simulation data...');
        
        try {
            const [topicsResponse, producersResponse, consumersResponse, consumerGroupsResponse] = await Promise.all([
                fetch('/api/topics').then(r => r.json()),
                fetch('/api/producers').then(r => r.json()),
                fetch('/api/consumers').then(r => r.json()),
                fetch('/api/consumer-groups').then(r => r.json())
            ]);

            // Handle new API response format
            const topics = topicsResponse.success ? topicsResponse.data : topicsResponse;
            const producers = producersResponse.success ? producersResponse.data : producersResponse;
            const consumers = consumersResponse.success ? consumersResponse.data : consumersResponse;
            const consumerGroups = consumerGroupsResponse.success ? consumerGroupsResponse.data : consumerGroupsResponse;

            topics.forEach(topic => this.topics.set(topic.id, topic));
            producers.forEach(producer => this.producers.set(producer.id, producer));
            consumers.forEach(consumer => this.consumers.set(consumer.id, consumer));
            consumerGroups.forEach(group => this.consumerGroups.set(group.id, group));

            this.renderAll();
            this.updateStats();
            this.hideLoadingOverlay();
        } catch (error) {
            console.error('Error loading initial data:', error);
            this.hideLoadingOverlay();
            this.showError('Failed to load initial data. Please refresh the page.');
        }
    }

    showLoadingOverlay(message = 'Loading...') {
        const overlay = document.createElement('div');
        overlay.id = 'loadingOverlay';
        overlay.className = 'loading-overlay';
        overlay.innerHTML = `
            <div class="text-center">
                <div class="loading-spinner"></div>
                <div class="loading-text">${message}</div>
            </div>
        `;
        document.body.appendChild(overlay);
    }

    hideLoadingOverlay() {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.remove();
        }
    }

    showSkeletonCards(containerId, count = 3) {
        const container = document.getElementById(containerId);
        if (!container) return;

        container.innerHTML = '';
        for (let i = 0; i < count; i++) {
            const skeletonCard = document.createElement('div');
            skeletonCard.className = 'skeleton-card';
            skeletonCard.innerHTML = `
                <div class="skeleton skeleton-circle"></div>
                <div class="skeleton skeleton-line short"></div>
                <div class="skeleton skeleton-line medium"></div>
                <div class="skeleton skeleton-line long"></div>
            `;
            container.appendChild(skeletonCard);
        }
    }

    showError(message) {
        // Create a simple error notification
        const errorDiv = document.createElement('div');
        errorDiv.className = 'error-notification';
        errorDiv.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: #f56565;
            color: white;
            padding: 1rem 1.5rem;
            border-radius: 8px;
            box-shadow: 0 4px 20px rgba(245, 101, 101, 0.3);
            z-index: 1000;
            font-weight: 500;
        `;
        errorDiv.textContent = message;
        
        document.body.appendChild(errorDiv);
        
        // Remove after 5 seconds
        setTimeout(() => {
            if (errorDiv.parentNode) {
                errorDiv.parentNode.removeChild(errorDiv);
            }
        }, 5000);
    }

    renderAll() {
        this.renderProducers();
        this.renderTopics();
        this.renderConsumers();
        this.renderConsumerGroups();
        this.populateReplayTopics();
        this.populateMessageFilters();
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

    renderConsumerGroups() {
        const container = document.getElementById('consumerGroupsContainer');
        container.innerHTML = '';

        this.consumerGroups.forEach(group => {
            const groupCard = this.createConsumerGroupCard(group);
            container.appendChild(groupCard);
        });
    }

    createConsumerGroupCard(group) {
        const card = document.createElement('div');
        card.className = 'consumer-group-card';
        card.dataset.groupId = group.id;

        const topicsList = group.topics.join(', ');
        const members = group.consumers.map(consumerId => {
            const consumer = this.consumers.get(consumerId);
            return consumer ? consumer.name : consumerId;
        });

        card.innerHTML = `
            <div class="consumer-group-header">
                <div class="consumer-group-name">${group.name}</div>
                <div class="consumer-group-id">${group.id}</div>
            </div>
            <div class="consumer-group-topics">Topics: ${topicsList}</div>
            <div class="consumer-group-stats">
                <span>Processed: ${group.totalProcessedMessages}</span>
                <span>Members: ${group.consumers.length}</span>
            </div>
            <div class="consumer-group-members">
                ${members.map(member => `<span class="consumer-member">${member}</span>`).join('')}
            </div>
        `;

        return card;
    }

    handleMessageProduced(data) {
        const { message, producer, topic } = data;
        
        // Track message for latency calculation
        message.producedTime = Date.now();
        this.messages.push(message);
        
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
        
        // Calculate latency
        if (message.producedTime) {
            const latency = Date.now() - message.producedTime;
            this.latencyData.push(latency);
            
            // Keep only last 1000 latency measurements
            if (this.latencyData.length > 1000) {
                this.latencyData = this.latencyData.slice(-1000);
            }
        }
        
        // Update consumer card
        const consumerCard = document.querySelector(`[data-consumer-id="${consumer.id}"]`);
        if (consumerCard) {
            consumerCard.classList.add('active');
            consumerCard.querySelector('.consumer-stats span').textContent = `Processed: ${consumer.processedMessages}`;
            setTimeout(() => consumerCard.classList.remove('active'), 2000);
        }

        // Update consumer group card if applicable
        if (data.groupId && data.groupId !== 'default') {
            const groupCard = document.querySelector(`[data-group-id="${data.groupId}"]`);
            if (groupCard) {
                groupCard.classList.add('active');
                const group = this.consumerGroups.get(data.groupId);
                if (group) {
                    groupCard.querySelector('.consumer-group-stats span').textContent = `Processed: ${group.totalProcessedMessages}`;
                }
                setTimeout(() => groupCard.classList.remove('active'), 2000);
            }
        }

        // Show message flow animation
        this.showMessageFlowAnimation(message, 'topic', 'consumer');
        
        // Update stats
        this.updateStats();
    }

    updateCharts() {
        const now = new Date();
        const timeLabel = now.toLocaleTimeString();
        
        // Update throughput chart
        const messagesLastSecond = this.messages.filter(msg => 
            msg.producedTime && (now - msg.producedTime) < 1000
        ).length;
        
        this.throughputData.push(messagesLastSecond);
        
        // Keep only last 30 data points (30 seconds)
        if (this.throughputData.length > 30) {
            this.throughputData = this.throughputData.slice(-30);
        }
        
        // Update chart data
        this.charts.throughput.data.labels = Array.from({ length: this.throughputData.length }, (_, i) => {
            const time = new Date(now - (this.throughputData.length - i - 1) * 1000);
            return time.toLocaleTimeString();
        });
        this.charts.throughput.data.datasets[0].data = [...this.throughputData];
        this.charts.throughput.update('none');
        
        // Update latency chart
        if (this.latencyData.length > 0) {
            const latencyBuckets = [0, 0, 0, 0, 0]; // < 100ms, 100-500ms, 500ms-1s, 1-2s, > 2s
            
            this.latencyData.forEach(latency => {
                if (latency < 100) latencyBuckets[0]++;
                else if (latency < 500) latencyBuckets[1]++;
                else if (latency < 1000) latencyBuckets[2]++;
                else if (latency < 2000) latencyBuckets[3]++;
                else latencyBuckets[4]++;
            });
            
            this.charts.latency.data.datasets[0].data = latencyBuckets;
            this.charts.latency.update('none');
        }
    }

    showMessageFlowAnimation(message, from, to) {
        const container = document.getElementById('messageFlowContainer');
        container.classList.add('active');

        // Create a more sophisticated animation element
        const animationElement = document.createElement('div');
        animationElement.className = 'message-flow-animation';
        
        // Determine animation path based on message flow direction
        const startY = Math.random() * 120 + 40;
        const endY = Math.random() * 120 + 40;
        
        // Create message content with more details
        const messageType = message.data.type || 'data';
        const messageIcon = this.getMessageIcon(messageType);
        
        animationElement.innerHTML = `
            <div class="message-animation-content">
                <div class="message-icon">${messageIcon}</div>
                <div class="message-info">
                    <div class="message-type">${messageType}</div>
                    <div class="message-id">${message.id.substring(0, 6)}</div>
                </div>
            </div>
        `;
        
        animationElement.style.cssText = `
            position: absolute;
            top: ${startY}px;
            left: -120px;
            width: 100px;
            height: 50px;
            background: linear-gradient(135deg, #667eea, #764ba2);
            border-radius: 25px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 0.7rem;
            font-weight: 500;
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
            z-index: 10;
            transition: all 0.1s ease;
        `;

        container.appendChild(animationElement);

        // Animate the message flow with more realistic movement
        let progress = 0;
        const animationDuration = 2500;
        const startTime = Date.now();
        
        const animate = () => {
            const elapsed = Date.now() - startTime;
            progress = Math.min(elapsed / animationDuration, 1);
            
            // Easing function for smooth animation
            const easeInOutCubic = progress < 0.5 
                ? 4 * progress * progress * progress 
                : 1 - Math.pow(-2 * progress + 2, 3) / 2;
            
            const currentX = -120 + (container.offsetWidth + 120) * easeInOutCubic;
            const currentY = startY + (endY - startY) * easeInOutCubic;
            
            animationElement.style.left = `${currentX}px`;
            animationElement.style.top = `${currentY}px`;
            
            // Add rotation effect
            animationElement.style.transform = `rotate(${progress * 360}deg)`;
            
            // Add pulsing effect
            const pulseScale = 1 + Math.sin(progress * Math.PI * 4) * 0.1;
            animationElement.style.transform += ` scale(${pulseScale})`;
            
            if (progress < 1) {
                requestAnimationFrame(animate);
            } else {
        // Remove animation element after completion
            if (animationElement.parentNode) {
                animationElement.parentNode.removeChild(animationElement);
            }
            }
        };
        
        requestAnimationFrame(animate);

        // Remove active class after animation
        setTimeout(() => {
            container.classList.remove('active');
        }, animationDuration);
    }

    getMessageIcon(messageType) {
        const iconMap = {
            'login': '🔐',
            'page_view': '👁️',
            'click': '👆',
            'logout': '🚪',
            'order_created': '🛒',
            'payment_processed': '💳',
            'order_shipped': '📦',
            'order_delivered': '✅',
            'conversion': '📈',
            'bounce': '↩️',
            'engagement': '🎯',
            'payment_initiated': '💰',
            'payment_authorized': '🔒',
            'payment_completed': '✅',
            'stock_reduced': '📉',
            'restock': '📈',
            'low_stock': '⚠️',
            'default': '📨'
        };
        
        return iconMap[messageType] || iconMap.default;
    }

    addMessageToHistory(message, type) {
        const container = document.getElementById('messageHistoryContainer');
        
        const messageElement = document.createElement('div');
        messageElement.className = `message-item ${type === 'produced' ? 'new' : ''}`;
        messageElement.dataset.messageData = JSON.stringify(message);
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

        // Apply current filters to new message
        this.applyMessageFilters();

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
            const response = await fetch('/api/demo/start', { method: 'POST' });
            const result = await response.json();
            
            if (!result.success) {
                this.showError(result.error || 'Failed to start demo');
            }
        } catch (error) {
            console.error('Error starting demo:', error);
            this.showError('Failed to start demo. Please try again.');
        }
    }

    async nextStep() {
        try {
            const response = await fetch('/api/demo/step', { method: 'POST' });
            const result = await response.json();
            
            if (!result.success) {
                this.showError(result.error || 'Failed to advance demo step');
            }
        } catch (error) {
            console.error('Error advancing demo step:', error);
            this.showError('Failed to advance demo step. Please try again.');
        }
    }

    async startAutoDemo() {
        try {
            const response = await fetch('/api/demo/auto', { method: 'POST' });
            const result = await response.json();
            
            if (!result.success) {
                this.showError(result.error || 'Failed to start auto demo');
            }
        } catch (error) {
            console.error('Error starting auto demo:', error);
            this.showError('Failed to start auto demo. Please try again.');
        }
    }

    async stopDemo() {
        try {
            const response = await fetch('/api/demo/stop', { method: 'POST' });
            const result = await response.json();
            
            if (!result.success) {
                this.showError(result.error || 'Failed to stop demo');
            }
        } catch (error) {
            console.error('Error stopping demo:', error);
            this.showError('Failed to stop demo. Please try again.');
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
            const response = await fetch(`/api/producer/${producer.id}/send`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: randomMessage,
                    topic: producer.topic
                })
            });
            
            const result = await response.json();
            
            if (!result.success) {
                this.showError(result.error || 'Failed to send message');
            }
        } catch (error) {
            console.error('Error sending message:', error);
            this.showError('Failed to send message. Please try again.');
        }
    }

    // Advanced control methods
    async togglePersistence() {
        try {
            const response = await fetch('/api/persistence/toggle', { method: 'POST' });
            const result = await response.json();
            
            if (result.success) {
                this.showSuccess(result.message);
                this.updatePersistenceStatus(result.persistenceEnabled);
            } else {
                this.showError(result.error || 'Failed to toggle persistence');
            }
        } catch (error) {
            console.error('Error toggling persistence:', error);
            this.showError('Failed to toggle persistence. Please try again.');
        }
    }

    async persistData() {
        try {
            const response = await fetch('/api/data/persist', { method: 'POST' });
            const result = await response.json();
            
            if (result.success) {
                this.showSuccess(result.message);
            } else {
                this.showError(result.error || 'Failed to persist data');
            }
        } catch (error) {
            console.error('Error persisting data:', error);
            this.showError('Failed to persist data. Please try again.');
        }
    }

    async clearData() {
        if (!confirm('Are you sure you want to clear all data? This action cannot be undone.')) {
            return;
        }

        try {
            const response = await fetch('/api/data/clear', { method: 'POST' });
            const result = await response.json();
            
            if (result.success) {
                this.showSuccess(result.message);
                // Reload the page to reset the UI
                setTimeout(() => {
                    window.location.reload();
                }, 2000);
            } else {
                this.showError(result.error || 'Failed to clear data');
            }
        } catch (error) {
            console.error('Error clearing data:', error);
            this.showError('Failed to clear data. Please try again.');
        }
    }

    async replayMessages() {
        const topicSelect = document.getElementById('replayTopic');
        const fromOffset = document.getElementById('fromOffset').value;
        const toOffset = document.getElementById('toOffset').value;

        if (!topicSelect.value) {
            this.showError('Please select a topic to replay messages from');
            return;
        }

        try {
            const response = await fetch(`/api/replay/${topicSelect.value}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    fromOffset: fromOffset ? parseInt(fromOffset) : 0,
                    toOffset: toOffset ? parseInt(toOffset) : null
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.showSuccess(result.message);
            } else {
                this.showError(result.error || 'Failed to replay messages');
            }
        } catch (error) {
            console.error('Error replaying messages:', error);
            this.showError('Failed to replay messages. Please try again.');
        }
    }

    showSuccess(message) {
        const successDiv = document.createElement('div');
        successDiv.className = 'success-notification';
        successDiv.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: #48bb78;
            color: white;
            padding: 1rem 1.5rem;
            border-radius: 8px;
            box-shadow: 0 4px 20px rgba(72, 187, 120, 0.3);
            z-index: 1000;
            font-weight: 500;
        `;
        successDiv.textContent = message;
        
        document.body.appendChild(successDiv);
        
        // Remove after 3 seconds
        setTimeout(() => {
            if (successDiv.parentNode) {
                successDiv.parentNode.removeChild(successDiv);
            }
        }, 3000);
    }

    updatePersistenceStatus(enabled) {
        const toggleBtn = document.getElementById('togglePersistence');
        const icon = toggleBtn.querySelector('i');
        
        if (enabled) {
            icon.className = 'fas fa-save';
            toggleBtn.innerHTML = '<i class="fas fa-save"></i> Disable Persistence';
        } else {
            icon.className = 'fas fa-ban';
            toggleBtn.innerHTML = '<i class="fas fa-ban"></i> Enable Persistence';
        }
    }

    populateReplayTopics() {
        const topicSelect = document.getElementById('replayTopic');
        topicSelect.innerHTML = '<option value="">Select Topic</option>';
        
        this.topics.forEach(topic => {
            const option = document.createElement('option');
            option.value = topic.id;
            option.textContent = topic.name;
            topicSelect.appendChild(option);
        });
    }

    // Message filtering methods
    filterMessages() {
        const searchTerm = document.getElementById('messageSearch').value.toLowerCase();
        const topicFilter = document.getElementById('messageFilter').value;
        const typeFilter = document.getElementById('messageTypeFilter').value;

        this.currentFilters = {
            search: searchTerm,
            topic: topicFilter,
            type: typeFilter
        };

        this.applyMessageFilters();
    }

    applyMessageFilters() {
        const container = document.getElementById('messageHistoryContainer');
        const messageElements = Array.from(container.children);

        messageElements.forEach(element => {
            const messageData = element.dataset.messageData ? JSON.parse(element.dataset.messageData) : null;
            if (!messageData) return;

            const matchesSearch = !this.currentFilters.search || 
                JSON.stringify(messageData.data).toLowerCase().includes(this.currentFilters.search) ||
                messageData.id.toLowerCase().includes(this.currentFilters.search) ||
                messageData.producerId.toLowerCase().includes(this.currentFilters.search);

            const matchesTopic = !this.currentFilters.topic || messageData.topic === this.currentFilters.topic;

            const matchesType = !this.currentFilters.type || 
                (messageData.data.type && messageData.data.type.includes(this.currentFilters.type)) ||
                (this.currentFilters.type === 'iot' && messageData.topic === 'iot-sensors');

            const shouldShow = matchesSearch && matchesTopic && matchesType;
            element.style.display = shouldShow ? 'block' : 'none';
        });

        // Update filter counts
        this.updateFilterCounts();
    }

    clearMessageFilters() {
        document.getElementById('messageSearch').value = '';
        document.getElementById('messageFilter').value = '';
        document.getElementById('messageTypeFilter').value = '';

        this.currentFilters = {
            search: '',
            topic: '',
            type: ''
        };

        this.applyMessageFilters();
    }

    updateFilterCounts() {
        const container = document.getElementById('messageHistoryContainer');
        const visibleMessages = Array.from(container.children).filter(el => el.style.display !== 'none');
        
        // Update header with count
        const header = document.querySelector('.message-history-header h3');
        const originalText = header.textContent.replace(/\(\d+\)/, '');
        header.textContent = `${originalText} (${visibleMessages.length})`;
    }

    populateMessageFilters() {
        const topicFilter = document.getElementById('messageFilter');
        topicFilter.innerHTML = '<option value="">All Topics</option>';
        
        this.topics.forEach(topic => {
            const option = document.createElement('option');
            option.value = topic.id;
            option.textContent = topic.name;
            topicFilter.appendChild(option);
        });
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
    console.log('🔧 Use advanced controls for persistence and replay');
});
