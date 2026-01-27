package main

import (
	"log"
	"net/http"
	"time"

	"github.com/joho/godotenv"

	"macroquant-intel/backend-go/internal/config"
	internalhttp "macroquant-intel/backend-go/internal/http"
	"macroquant-intel/backend-go/internal/services"
)

func main() {
	_ = godotenv.Load(
		".env",
		".env.local",
		"../.env",
		"../.env.local",
		"backend-go/.env",
		"backend-go/.env.local",
	)
	cfg := config.Load()
	cache := services.NewCache(cfg)
	py := services.NewPythonClient(cfg)

	h := internalhttp.NewRouter(cfg, cache, py)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("macroquant backend listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
