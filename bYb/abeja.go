package main
import (
	"fmt"
	"github.com/streadway/amqp"
	"os"
	"log"
	"time"
)

const(
	NUM_MIEL int = 30;
	NOM_COLA_ABE_IN string = "AbejasIn";
	NOM_COLA_ABE_OUT string = "AbejasOut";
	URI string = "amqp://guest:guest@localhost:5672/"
)

func main(){
	
	nom := os.Args[len(os.Args)-1];
	fmt.Printf("Soy la abeja %s \n", nom);

	//////////////Iniciar conexion para enviar miel////////////
	connOut, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connOut.Close()
	channelOut, err := connOut.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelOut.Close()
	colaOut, err := channelOut.QueueDeclare(
		NOM_COLA_ABE_OUT, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	
	//////////////Iniciar conexion para recibir msg////////////
	connIn, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connIn.Close()
	channelIn, err := connIn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelIn.Close()
	colaIn, err := channelIn.QueueDeclare(
		NOM_COLA_ABE_IN, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	
	canalIn,_ := channelIn.Consume(colaIn.Name,"",false,false,false,false,nil);
	fmt.Println("Voy a comenzar a producir... ");
	
	for i := 1;; i++ {
				
		fmt.Printf("Soy la abeja %s y produzco la pieza de miel nº %d\n", nom , i);
		enviarMsg(channelOut, colaOut, nom);  //envia miel al bote
		<-canalIn;  //espera a que el bote le envíe el msg de continuar
		time.Sleep(2*time.Second);
	}
	
}

func enviarMsg(ch *amqp.Channel, cola amqp.Queue, msg string ){
	err := ch.Publish(
		"",     // exchange
		cola.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})
	failOnError(err, "Failed to publish a message")
}



func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s", msg, err)
    panic(fmt.Sprintf("%s: %s", msg, err))
  }
}



