package main
import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
	"log"
)

const(
	NUM_MIEL int = 20
	NOM_COLA_OSO_IN string = "OsoIn"
	NOM_COLA_OSO_OUT string = "OsoOut"
	URI string = "amqp://guest:guest@localhost:5672/"
)

func main(){
		
	connIn, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connIn.Close()
	channelIn, err := connIn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelIn.Close()
	colaIn, err := channelIn.QueueDeclare(NOM_COLA_OSO_IN, false,false,false,false,nil,)
	failOnError(err, "Failed to declare a queue")
	canalIn, err := channelIn.Consume(colaIn.Name,"", true, false,false, false, nil)
	failOnError(err, "Failed to register a consumer")
	
	connOut, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connOut.Close()
	channelOut, err := connIn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelOut.Close()
	colaOut, err := channelOut.QueueDeclare(NOM_COLA_OSO_OUT, false,false,false,false,nil,)
	failOnError(err, "Failed to declare a queue")
	
	fmt.Println("Durmiendo... ");
	for true{
		msg := <-canalIn;
		fmt.Println("Me ha despertado la abeja", string(msg.Body[:]));
		fmt.Println("Comiendo...");
		time.Sleep(10*time.Second);
		fmt.Println("Durmiendo...");
		
		enviarMsg(channelOut, colaOut, "");  //enviar msg al bote para abisarle de que ya he comido el bote de miel
	}	
}

func enviarMsg(ch *amqp.Channel, cola amqp.Queue, msg string ){
	_ = ch.Publish(
		"",        // exchange
		cola.Name, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		});
}
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
