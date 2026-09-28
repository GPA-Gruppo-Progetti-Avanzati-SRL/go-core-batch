package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// Il Payload di un WorkItem è `any`, e la forma in cui torna indietro dipende da CHI l'ha riletto:
// il driver Mongo restituisce un documento come lista ordinata di coppie (bson.D) o come mappa
// (bson.M), una colonna jsonb di SQL come map[string]any o []byte, e chi l'ha appena costruito ce
// l'ha ancora come struct. Chi lo consuma deve normalizzarlo, e finora se lo scriveva da sé: c'era
// un convertitore in scheduler/kafkajob e un altro in scheduler/distributedjob/s3feed, con
// semantiche diverse — quello di s3feed passava per json.Marshal, che su una lista di coppie
// produce un ARRAY e non un oggetto, quindi il job DistribuiteTaskByS3File non decodificava il
// proprio payload quando il backend era Mongo.
//
// La conversione sta qui perché è una proprietà del WorkItem, non dei suoi consumatori.

// PayloadMap porta il Payload a map[string]any qualunque sia la forma in cui il backend l'ha
// restituito. Ritorna (nil,false) se il payload non è un documento — un numero, una lista, una
// stringa che non è JSON valido — perché non è una condizione di errore ma un payload di un'altra
// specie, e chi chiama decide cosa farne.
func PayloadMap(p any) (map[string]any, bool) {
	switch v := p.(type) {
	case nil:
		return nil, false
	case map[string]any:
		return v, true
	case []byte:
		return jsonMap(v)
	case string:
		return jsonMap([]byte(v))
	}
	m, ok := toNative(p).(map[string]any)
	return m, ok
}

// DecodePayload decodifica il Payload nel tipo dell'applicazione. out dev'essere un puntatore.
//
// Se il payload è GIÀ del tipo richiesto (non è mai passato da un backend: l'ha appena costruito
// chi lo sta leggendo) viene assegnato direttamente; altrimenti si normalizza e si passa da JSON,
// che è l'unico traduttore che conosce i tag dei campi.
func DecodePayload(raw any, out any) error {
	if raw == nil {
		return errors.New("payload assente")
	}
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("destinazione non valida: atteso un puntatore non nil, ricevuto %T", out)
	}
	dst := rv.Elem()
	src := reflect.ValueOf(raw)
	if src.Type() == dst.Type() {
		dst.Set(src)
		return nil
	}
	if src.Kind() == reflect.Pointer && !src.IsNil() && src.Elem().Type() == dst.Type() {
		dst.Set(src.Elem())
		return nil
	}

	var payload any = toNative(raw)
	switch v := raw.(type) {
	case []byte:
		return json.Unmarshal(v, out)
	case string:
		return json.Unmarshal([]byte(v), out)
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("payload di tipo non gestito %T: %w", raw, err)
	}
	return json.Unmarshal(b, out)
}

func jsonMap(b []byte) (map[string]any, bool) {
	var m map[string]any
	if json.Unmarshal(b, &m) != nil {
		return nil, false
	}
	return m, true
}

// toNative ricostruisce ricorsivamente contenitori e scalari in tipi nativi, LASCIANDO INVARIATI
// i valori: un int64 resta un int64, non diventa un float64 passando da JSON. Serve perché il
// payload di un WorkItem finisce su Kafka come chiave di partizionamento, e un intero grande che
// cambia rappresentazione cambierebbe la partizione.
func toNative(v any) any {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case []byte, string, bool, time.Time,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		return v
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return toNative(rv.Elem().Interface())
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return v
		}
		m := make(map[string]any, rv.Len())
		for iter := rv.MapRange(); iter.Next(); {
			m[iter.Key().String()] = toNative(iter.Value().Interface())
		}
		return m
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return v // []byte e i suoi tipi derivati restano byte
		}
		if m, ok := pairsToMap(rv); ok {
			return m
		}
		a := make([]any, rv.Len())
		for i := range a {
			a[i] = toNative(rv.Index(i).Interface())
		}
		return a
	default:
		return v
	}
}

// pairsToMap riconosce un documento espresso come LISTA ORDINATA di coppie, che è la forma con cui
// il driver Mongo restituisce un (sotto)documento: bson.D è []bson.E, e bson.E è
// struct{Key string; Value any}.
//
// È riconosciuto per FORMA e non per tipo di proposito: questo package è il seam pubblico che ogni
// applicazione importa, e nominare bson qui trascinerebbe il driver Mongo dentro le app SQL-only —
// che è esattamente ciò che la separazione store/mongostore e store/sqlstore esiste per evitare.
func pairsToMap(rv reflect.Value) (map[string]any, bool) {
	et := rv.Type().Elem()
	if et.Kind() != reflect.Struct || et.NumField() != 2 {
		return nil, false
	}
	k, v := et.Field(0), et.Field(1)
	if k.Name != "Key" || k.Type.Kind() != reflect.String {
		return nil, false
	}
	if v.Name != "Value" || v.Type.Kind() != reflect.Interface {
		return nil, false
	}
	m := make(map[string]any, rv.Len())
	for i := range rv.Len() {
		e := rv.Index(i)
		m[e.Field(0).String()] = toNative(e.Field(1).Interface())
	}
	return m, true
}
