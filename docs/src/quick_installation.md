## Quick installation

### Production

```
git submodule update
git clone https://github.com/kartoza/tomorrownow_gap
cp deployment/.template.env deployment/.env
cp deployment/docker-compose.override.template deployment/docker-compose.template
make up
```

The web will be available at `http://127.0.0.1/`

To stop containers:

```
make kill
```

To stop and delete containers:

```
make down
```

### Development

```
git submodule update
git clone https://github.com/kartoza/tomorrownow_gap
cp deployment/.template.env deployment/.env
cp deployment/docker-compose.override.template deployment/docker-compose.template
```

After that, do

- open new terminal
- on folder root of project, do

```
make serve
```

Wait until it is done
when there is sentence "webpack xxx compiled successfully in xxx ms".<br>
After that, don't close the terminal.
If it is accidentally closed, do `make serve` again

Next step:

- Open new terminal
- Do commands below

```
make up
make dev
```

Wait until it is on.

The web can be accessed using `http://localhost:5000/`

If the web is taking long time to load, restart cloud_native_gis_dev_1
container.<br>
The sequence should be `make dev`, after that run or restart
cloud_native_gis_dev_1.