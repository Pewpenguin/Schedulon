package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// LoadTLSCredentials builds gRPC transport credentials from PEM files.
//
// Certificate and key (certFile, keyFile) must both be set or both empty.
// At least one of a certificate/key pair or a CA file is required.
//
// Typical use:
//   - Server: non-empty cert and key; optional CA to require and verify client certificates (mTLS).
//   - Client: CA to verify the server; optional cert and key for a client certificate (mTLS).
func LoadTLSCredentials(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	hasCert := certFile != "" || keyFile != ""
	if hasCert && (certFile == "" || keyFile == "") {
		return nil, fmt.Errorf("tls: both cert and key files are required when specifying a certificate")
	}

	hasCA := caFile != ""
	if !hasCert && !hasCA {
		return nil, fmt.Errorf("tls: at least one of cert/key pair or CA file is required")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if hasCert {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: load X509 key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if hasCA {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls: no valid CA certificates in %s", caFile)
		}
		tlsConfig.RootCAs = pool
		tlsConfig.ClientCAs = pool
	}

	if hasCert && hasCA {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	} else if hasCert {
		tlsConfig.ClientAuth = tls.NoClientCert
	}

	return credentials.NewTLS(tlsConfig), nil
}

// ClientGRPCDialOptions returns dial options for a gRPC client: TLS when any TLS
// path is set, otherwise insecure credentials. A certificate requires a matching key.
func ClientGRPCDialOptions(certFile, keyFile, caFile string) ([]grpc.DialOption, error) {
	if certFile == "" && keyFile == "" && caFile == "" {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	if (certFile != "" || keyFile != "") && (certFile == "" || keyFile == "") {
		return nil, fmt.Errorf("tls: client requires both cert and key files when specifying a client certificate")
	}
	creds, err := LoadTLSCredentials(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(creds)}, nil
}
