# Kafka Visual Demo

A comprehensive visual demonstration of Apache Kafka functionality with multiple producers, topics, partitions, and consumers.

## Features

### 🎯 Core Kafka Concepts Visualization
- **Multiple Producers**: Web apps, mobile apps, IoT sensors, backend services
- **Topics & Partitions**: Visual representation of message distribution
- **Consumers**: Different consumer groups and processing patterns
- **Message Flow**: Real-time animation of messages flowing through the system

### 🎮 Interactive Demo Modes
- **Step-by-Step Demo**: Guided walkthrough of Kafka concepts
- **Auto Demo**: Continuous message generation and processing
- **Manual Testing**: Click producers to send test messages

### 📊 Real-time Dashboard
- Live statistics and metrics
- Message history with detailed views
- Producer and consumer activity indicators
- Partition-level message distribution

### 🎨 Modern UI/UX
- Responsive design for all screen sizes
- Smooth animations and transitions
- Interactive components with hover effects
- Professional color scheme and typography

## Quick Start

1. **Install Dependencies**
   ```bash
   npm install
   ```

2. **Start the Server**
   ```bash
   npm start
   ```

3. **Open in Browser**
   ```
   http://localhost:3000
   ```

## Development

```bash
# Development mode with auto-reload
npm run dev

# Build for production
npm run build
```

## Demo Modes

### Step-by-Step Demo
- Click "Start Demo" to begin guided walkthrough
- Use "Next Step" to advance through concepts
- Learn about topics, producers, partitions, and consumers

### Auto Demo
- Click "Auto Demo" for continuous message flow
- Watch real-time message processing
- See different producer types in action

### Manual Testing
- Click on producer cards to send test messages
- View message details by clicking history items
- Observe partition distribution and consumer processing

## Architecture

### Backend Components
- **Express Server**: REST API and WebSocket server
- **Kafka Simulator**: Core simulation engine
- **Socket.IO**: Real-time communication

### Frontend Components
- **Interactive Dashboard**: Statistics and controls
- **Visual Components**: Producers, topics, consumers
- **Message Flow**: Animated message processing
- **History Viewer**: Detailed message inspection

### Producer Types
- **Web Application**: User events and interactions
- **Mobile App**: Mobile-specific user actions
- **IoT Gateway**: Sensor data aggregation
- **Backend Services**: System-to-system communication
- **Payment Gateway**: Financial transactions
- **Inventory System**: Stock management events

### Consumer Types
- **Event Processors**: Handle specific event types
- **Analytics Processors**: Data analysis and reporting
- **Notification Services**: User notifications
- **Dashboard Consumers**: Real-time monitoring

## Keyboard Shortcuts

- **Space**: Start demo
- **Right Arrow**: Next step (during step-by-step demo)
- **Escape**: Close modal dialogs

## API Endpoints

- `GET /api/topics` - Get all topics
- `GET /api/producers` - Get all producers
- `GET /api/consumers` - Get all consumers
- `GET /api/messages/:topic` - Get messages for a topic
- `POST /api/producer/:id/send` - Send message from producer
- `POST /api/demo/start` - Start step-by-step demo
- `POST /api/demo/stop` - Stop demo
- `POST /api/demo/step` - Advance demo step
- `POST /api/demo/auto` - Start auto demo

## WebSocket Events

- `topicCreated` - New topic added
- `producerCreated` - New producer added
- `consumerCreated` - New consumer added
- `messageProduced` - Message sent to topic
- `messageConsumed` - Message processed by consumer
- `demoStarted` - Demo mode started
- `demoStopped` - Demo mode stopped
- `demoStep` - Demo step advanced
- `autoDemoStarted` - Auto demo started

## Technologies Used

- **Backend**: Node.js, Express, Socket.IO
- **Frontend**: Vanilla JavaScript, CSS3, HTML5
- **Styling**: Modern CSS with gradients and animations
- **Icons**: Font Awesome
- **Fonts**: Inter (Google Fonts)

## Educational Value

This demo helps understand:
- How Kafka topics organize messages
- How partitions enable parallel processing
- How producers send messages to topics
- How consumers process messages
- How message keys affect partition assignment
- How consumer groups work together
- Real-time message flow patterns

Perfect for:
- Learning Apache Kafka concepts
- Demonstrating Kafka architecture
- Training developers on event streaming
- Understanding microservices communication patterns

## License

MIT License - Feel free to use for educational purposes!
