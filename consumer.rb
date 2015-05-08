require 'poseidon'

consumer = Poseidon::PartitionConsumer.new("my_test_consumer", "localhost", 9092,
										"herokutest.tutors", 0, :earliest_offset)

loop do
	messages = consumer.fetch
	messages.each do |m|
		puts m.value
	end
end		
