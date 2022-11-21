package esstorage

type Config struct {
	Addresses []string `env:"ES_ADDRESSES"`
	UserName  string   `env:"ES_USER"`
	Password  string   `env:"ES_PASSWORD"`
}
