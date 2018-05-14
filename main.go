package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const db = "frota_znh"
const collectionLogs = "logsMonitrip"
const collectionBilhetes = "Bilhetes"
const urlDb = "localhost:27017"

const dataHoraInicial = "2017-05-04T00:00:00"
const dataHoraFinal = "2017-05-10T23:59:59"

const clienteID = 222

const userRabbit = "guest"
const passwordRabbit = "guest"
const hostRabbit = "localhost"
const portRabbit = 5672

func main() {
	fmt.Println("Abrindo conexao com o mongo...")
	session := connectDB()
	fmt.Println("Conexao com o mongo aberta com sucesso")
	fmt.Println("Abrindo conexao com o Rabbit...")
	channel := getAmqpChannel()
	fmt.Println("Conexao com o rabbit aberta com sucesso")

	fmt.Println("Obtendo bilhetes...")
	tickets := findTickets(session)
	fmt.Println("Quantidade de bilhetes obtidos", len(tickets))
	fmt.Println("Enviando bilhetes...")
	publish(tickets, channel)
	fmt.Println("Bilhetes enviados com sucesso")

	fmt.Println("Obtendo aberturas de viagem")
	openTriip := findOpenTriip(session)
	fmt.Println("Quantidade de aberturas de viagem obtidas", len(openTriip))
	fmt.Println("Enviando aberturas de viagem...")
	publish(openTriip, channel)
	fmt.Println("Aberturas de viagem envidas com sucesso.")

	fmt.Println("Obtendo fechamentos de viagem")
	closeTriip := findCloseTriip(session)
	fmt.Println("Quantidade de fechamentos de viagem obtidos", len(closeTriip))
	fmt.Println("Enviando fechamentos de viagem...")
	publish(closeTriip, channel)
	fmt.Println("Fechamentos de viagem envidos com sucesso.")

	fmt.Println("Obtendo outros logs")
	othersLogs := findOtherLogs(session)
	fmt.Println("Quantidade de outros logs obtidos", len(othersLogs))
	fmt.Println("Enviando outros logs...")
	publish(othersLogs, channel)
	fmt.Println("Outros logs envidos com sucesso.")

}

func findTickets(session *mgo.Session) []map[string]interface{} {
	collection := session.DB(db).C(collectionBilhetes)

	tickets := make([]map[string]interface{}, 0)

	err := collection.Find(
		bson.M{
			"clienteId":      clienteID,
			"dataHoraEvento": bson.M{"$gte": dataHoraInicial, "$lte": dataHoraFinal},
		},
	).Select(bson.M{"_id": 0, "_class": 0}).All(&tickets)

	if err != nil {
		log.Fatal("Erro ao obter bilhetes - ", err)
	}

	return tickets
}

func findOpenTriip(session *mgo.Session) []map[string]interface{} {
	collection := session.DB(db).C(collectionLogs)

	openTriips := make([]map[string]interface{}, 0)

	err := collection.Find(
		bson.M{
			"idCliente":          clienteID,
			"dataHoraEvento":     bson.M{"$gte": dataHoraInicial, "$lte": dataHoraFinal},
			"idLog":              7,
			"tipoRegistroViagem": 1,
		},
	).Select(bson.M{"_id": 0, "_class": 0}).All(&openTriips)

	if err != nil {
		log.Fatal("Erro ao obter abetura de viagens - ", err)
	}

	return openTriips
}

func findCloseTriip(session *mgo.Session) []map[string]interface{} {
	collection := session.DB(db).C(collectionLogs)

	closeTriips := make([]map[string]interface{}, 0)

	err := collection.Find(
		bson.M{
			"idCliente":          clienteID,
			"dataHoraEvento":     bson.M{"$gte": dataHoraInicial, "$lte": dataHoraFinal},
			"idLog":              7,
			"tipoRegistroViagem": 0,
		},
	).Select(bson.M{"_id": 0, "_class": 0}).All(&closeTriips)

	if err != nil {
		log.Fatal("Erro ao obter fechamento de viagens - ", err)
	}

	return closeTriips
}

func findOtherLogs(session *mgo.Session) []map[string]interface{} {

	collection := session.DB(db).C(collectionLogs)

	logs := make([]map[string]interface{}, 0)

	err := collection.Find(
		bson.M{
			"idCliente":      clienteID,
			"dataHoraEvento": bson.M{"$gte": dataHoraInicial, "$lte": dataHoraFinal},
			"idLog":          bson.M{"$ne": 7},
			"idJornada":      bson.M{"$exists": true},
			"idViagem":       bson.M{"$exists": true},
		},
	).Select(bson.M{"_id": 0, "_class": 0}).All(&logs)

	if err != nil {
		log.Fatal("Erro ao obter abetura de viagens - ", err)
	}

	return logs
}

func connectDB() *mgo.Session {

	session, err := mgo.Dial(urlDb)

	if err != nil {
		log.Fatal("Erro ao abrir conexao com o mongo - ", err)
	}

	return session
}

func publish(logs []map[string]interface{}, channel *amqp.Channel) {

	for _, log := range logs {

		bytes, err := json.Marshal(log)

		if err != nil {
			fmt.Printf("Erro ao converter log %s para json - ", log["idTransacao"])
			continue
		}

		err = channel.Publish(
			"",
			"monitriip.totalizadores",
			false,
			false,
			amqp.Publishing{
				ContentType:     "application/json",
				ContentEncoding: "utf-8",
				Body:            bytes,
			},
		)

		if err != nil {
			fmt.Println("Erro ao enviar log", log["idTransacao"], "para a fila")
		}
	}
}

func getAmqpChannel() *amqp.Channel {

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		userRabbit,
		passwordRabbit,
		hostRabbit,
		portRabbit,
	)

	conn, err := amqp.Dial(url)

	if err != nil {
		log.Fatal("Erro na conexão com o Rabbitmq -", err)
	}

	channel, err := conn.Channel()

	if err != nil {
		log.Fatal("Erro ao obter canal -", err)
	}

	return channel
}
