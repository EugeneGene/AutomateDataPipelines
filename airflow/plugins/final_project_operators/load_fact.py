from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):
                 

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.sql=sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('DELETE table')
        redshift.run("DROP TABLE IF EXISTS factSongPlays")
        redshift.run("""
                CREATE TABLE IF NOT EXISTS factSongPlays (
                    songplay_id  VARCHAR,
                    start_time   BIGINT NOT NULL REFERENCES dimTime(start_time),
                    user_id      BIGINT,
                    level        VARCHAR NOT NULL,
                    song_id      VARCHAR NOT NULL REFERENCES dimSongs(song_id),
                    artist_id    VARCHAR NOT NULL REFERENCES dimArtists(artist_id),
                    session_id   INT NOT NULL,
                    location     VARCHAR,
                    user_agent   TEXT NOT NULL);""")
        self.log.info('Copy Data')
        redshift.run(self.sql)
