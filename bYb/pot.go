package main

import (
	"log"
	"fmt"
	"github.com/streadway/amqp"
)
const (
	NOM_COLA_ABE_IN string = "AbejasIn"
	NOM_COLA_ABE_OUT string = "AbejasOut";
	NOM_COLA_OSO_IN string = "OsoIn";
	NOM_COLA_OSO_OUT string = "OsoOut";
	CAPACITY int = 10;

	URI string = "amqp://guest:guest@localhost:5672/"
	
	)
	
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	
	//Inicia la conexion entrante con la abeja///
	
	connIn, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connIn.Close()
	channelIn, err := connIn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelIn.Close()
	colaIn, err := channelIn.QueueDeclare(NOM_COLA_ABE_OUT, false,false,false,false,nil,)
	failOnError(err, "Failed to declare a queue")
	msgs, err := channelIn.Consume(colaIn.Name,"", true, false,false, false, nil)
	failOnError(err, "Failed to register a consumer")
	
	
	//Inicia la conexion saliente con la abeja///

	connOut, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connOut.Close()
	channelOut, err := connIn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelOut.Close()
	colaOut, err := channelOut.QueueDeclare(NOM_COLA_ABE_IN, false,false,false,false,nil,)
	failOnError(err, "Failed to declare a queue")
	
	//Inicia la conexion entrante con el oso///
	
	connFromOso, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connFromOso.Close()
	channelFromOso, err := connFromOso.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelFromOso.Close()
	colaFromOso, err := channelFromOso.QueueDeclare(NOM_COLA_OSO_OUT, false,false,false,false,nil,)
	failOnError(err, "Failed to declare a queue")
	canalOso, err := channelFromOso.Consume(colaFromOso.Name,"", true, false,false, false, nil)
	failOnError(err, "Failed to register a consumer")
	
	//Inicia la conexion saliente con el oso///

	connToOso, err := amqp.Dial(URI)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connToOso.Close()
	channelToOso, err := connToOso.Channel()
	failOnError(err, "Failed to open a channel")
	defer channelToOso.Close()
	colaToOso, err := channelToOso.QueueDeclare(NOM_COLA_OSO_IN, false,false,false,false,nil,)
	failOnError(err, "Failed to declare a queue")
	
	
	
	mielCount := 0
	pot := 1;
	for true{
		
		fmt.Printf("El pote %d con capacidad %d está listo para recibir miel\n" , pot, CAPACITY);
		msg := <-msgs  //recibe Miel de alguna abeja
		nomAbeja := string(msg.Body[:])
		mielCount += 1
		
		fmt.Printf("Se ha recibido la %dº pieza de miel de la abeja: %s\n", mielCount, nomAbeja)
		if(mielCount == CAPACITY){
			fmt.Println("La abeja ", nomAbeja, " va a despertar al oso.");
			enviarMsg(channelToOso, colaToOso,nomAbeja);	//envia un mensaje al oso para despertarlo		
			<-canalOso  //recibe el mensaje del oso cuando este ha acabado de comer
			mielCount = 0;
			pot += 1		
		}
		enviarMsg(channelOut, colaOut,"") //envia msg a la abeja para que continue la produccion						
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


