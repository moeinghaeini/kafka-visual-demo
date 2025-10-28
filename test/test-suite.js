// Comprehensive Test Suite for Kafka Visual Demo
const KafkaSimulator = require('../kafka-simulator');

class TestSuite {
    constructor() {
        this.tests = [];
        this.passed = 0;
        this.failed = 0;
        this.simulator = null;
    }

    // Test runner
    async runTests() {
        console.log('🚀 Starting Kafka Visual Demo Test Suite...\n');
        
        // Initialize simulator
        this.simulator = new KafkaSimulator({ emit: () => {} });
        
        // Run all tests
        await this.testKafkaSimulator();
        await this.testMessageFlow();
        await this.testConsumerGroups();
        await this.testDeadLetterQueues();
        await this.testPersistence();
        await this.testMessageOrdering();
        
        // Print results
        this.printResults();
    }

    // Test helper
    async test(name, testFn) {
        try {
            await testFn();
            this.passed++;
            console.log(`✅ ${name}`);
        } catch (error) {
            this.failed++;
            console.log(`❌ ${name}: ${error.message}`);
        }
    }

    // Kafka Simulator Tests
    async testKafkaSimulator() {
        console.log('📊 Testing Kafka Simulator...');
        
        await this.test('Should create topics', () => {
            const topics = this.simulator.getTopics();
            if (topics.length === 0) throw new Error('No topics created');
        });

        await this.test('Should create producers', () => {
            const producers = this.simulator.getProducers();
            if (producers.length === 0) throw new Error('No producers created');
        });

        await this.test('Should create consumers', () => {
            const consumers = this.simulator.getConsumers();
            if (consumers.length === 0) throw new Error('No consumers created');
        });

        await this.test('Should create consumer groups', () => {
            const groups = this.simulator.getConsumerGroups();
            if (groups.length === 0) throw new Error('No consumer groups created');
        });

        await this.test('Should create dead letter queues', () => {
            const dlqs = this.simulator.getDeadLetterQueues();
            if (dlqs.length === 0) throw new Error('No dead letter queues created');
        });
    }

    // Message Flow Tests
    async testMessageFlow() {
        console.log('\n📨 Testing Message Flow...');
        
        await this.test('Should send message successfully', () => {
            const producer = this.simulator.getProducers()[0];
            const topic = this.simulator.getTopics()[0];
            
            const initialCount = topic.messageCount;
            this.simulator.sendMessage(producer.id, topic.id, { test: 'data' });
            
            if (topic.messageCount <= initialCount) {
                throw new Error('Message count did not increase');
            }
        });

        await this.test('Should handle message with key', () => {
            const producer = this.simulator.getProducers()[0];
            const topic = this.simulator.getTopics()[0];
            
            const initialCount = topic.messageCount;
            this.simulator.sendMessage(producer.id, topic.id, { 
                test: 'data', 
                key: 'test-key' 
            });
            
            if (topic.messageCount <= initialCount) {
                throw new Error('Message with key not processed');
            }
        });

        await this.test('Should assign correct partition', () => {
            const producer = this.simulator.getProducers()[0];
            const topic = this.simulator.getTopics()[0];
            
            this.simulator.sendMessage(producer.id, topic.id, { 
                test: 'data', 
                key: 'consistent-key' 
            });
            
            const messages = this.simulator.getMessages(topic.id);
            const message = messages[messages.length - 1];
            
            if (message.partition < 0 || message.partition >= topic.partitions) {
                throw new Error('Invalid partition assignment');
            }
        });
    }

    // Consumer Groups Tests
    async testConsumerGroups() {
        console.log('\n👥 Testing Consumer Groups...');
        
        await this.test('Should create consumer groups with correct structure', () => {
            const groups = this.simulator.getConsumerGroups();
            const group = groups[0];
            
            if (!group.id || !group.name || !Array.isArray(group.consumers)) {
                throw new Error('Invalid consumer group structure');
            }
        });

        await this.test('Should assign consumers to groups', () => {
            const consumers = this.simulator.getConsumers();
            const groups = this.simulator.getConsumerGroups();
            
            const consumersWithGroups = consumers.filter(c => c.groupId);
            if (consumersWithGroups.length === 0) {
                throw new Error('No consumers assigned to groups');
            }
        });

        await this.test('Should update group stats on message processing', () => {
            const group = this.simulator.getConsumerGroups()[0];
            const initialCount = group.totalProcessedMessages;
            
            // Simulate message processing
            const message = {
                id: 'test-message',
                topic: group.topics[0],
                data: { test: 'data' },
                timestamp: new Date()
            };
            
            this.simulator.processMessage(message);
            
            if (group.totalProcessedMessages <= initialCount) {
                throw new Error('Group stats not updated');
            }
        });
    }

    // Dead Letter Queue Tests
    async testDeadLetterQueues() {
        console.log('\n💀 Testing Dead Letter Queues...');
        
        await this.test('Should create dead letter queues', () => {
            const dlqs = this.simulator.getDeadLetterQueues();
            if (dlqs.length === 0) throw new Error('No dead letter queues created');
        });

        await this.test('Should send failed messages to DLQ', () => {
            const dlq = this.simulator.getDeadLetterQueues()[0];
            const initialCount = dlq.messages.length;
            
            // Create a message that will fail processing
            const message = {
                id: 'test-failed-message',
                topic: 'user-events',
                data: { test: 'data' },
                timestamp: new Date()
            };
            
            // Mock Math.random to force failure
            const originalRandom = Math.random;
            Math.random = () => 0.01; // 1% chance, should fail
            
            this.simulator.processMessage(message);
            
            // Restore Math.random
            Math.random = originalRandom;
            
            if (dlq.messages.length <= initialCount) {
                throw new Error('Failed message not sent to DLQ');
            }
        });
    }

    // Persistence Tests
    async testPersistence() {
        console.log('\n💾 Testing Persistence...');
        
        await this.test('Should enable persistence by default', () => {
            const stats = this.simulator.getStats();
            if (!stats.persistenceEnabled) {
                throw new Error('Persistence not enabled by default');
            }
        });

        await this.test('Should toggle persistence', () => {
            const initial = this.simulator.getStats().persistenceEnabled;
            this.simulator.togglePersistence();
            const toggled = this.simulator.getStats().persistenceEnabled;
            
            if (initial === toggled) {
                throw new Error('Persistence toggle not working');
            }
        });

        await this.test('Should persist data', () => {
            // This test would require file system access
            // For now, just test that the method exists and doesn't throw
            try {
                this.simulator.persistData();
            } catch (error) {
                throw new Error('Persist data method failed');
            }
        });
    }

    // Message Ordering Tests
    async testMessageOrdering() {
        console.log('\n📋 Testing Message Ordering...');
        
        await this.test('Should assign sequence numbers', () => {
            const producer = this.simulator.getProducers()[0];
            const topic = this.simulator.getTopics()[0];
            
            this.simulator.sendMessage(producer.id, topic.id, { test: 'data1' });
            this.simulator.sendMessage(producer.id, topic.id, { test: 'data2' });
            
            const messages = this.simulator.getMessages(topic.id);
            const lastMessage = messages[messages.length - 1];
            
            if (typeof lastMessage.sequenceNumber !== 'number') {
                throw new Error('Sequence number not assigned');
            }
        });

        await this.test('Should maintain offset order', () => {
            const producer = this.simulator.getProducers()[0];
            const topic = this.simulator.getTopics()[0];
            
            this.simulator.sendMessage(producer.id, topic.id, { test: 'data1' });
            this.simulator.sendMessage(producer.id, topic.id, { test: 'data2' });
            
            const messages = this.simulator.getMessages(topic.id);
            const lastTwo = messages.slice(-2);
            
            if (lastTwo[0].offset >= lastTwo[1].offset) {
                throw new Error('Offset order not maintained');
            }
        });
    }

    // Print test results
    printResults() {
        console.log('\n📊 Test Results:');
        console.log(`✅ Passed: ${this.passed}`);
        console.log(`❌ Failed: ${this.failed}`);
        console.log(`📈 Total: ${this.passed + this.failed}`);
        console.log(`🎯 Success Rate: ${((this.passed / (this.passed + this.failed)) * 100).toFixed(1)}%`);
        
        if (this.failed === 0) {
            console.log('\n🎉 All tests passed! The Kafka Visual Demo is working perfectly.');
        } else {
            console.log('\n⚠️  Some tests failed. Please check the implementation.');
        }
    }
}

// Run tests if this file is executed directly
if (require.main === module) {
    const testSuite = new TestSuite();
    testSuite.runTests().catch(console.error);
}

module.exports = TestSuite;
