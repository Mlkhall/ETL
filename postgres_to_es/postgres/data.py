from typing import List, Union
from pydantic import BaseModel


class ESData(BaseModel):
    id: str
    imdb_rating: float
    genre: Union[List[str], str, None]
    title: Union[List[str], str, None]
    description: Union[List[str], str, None]
    director: Union[List[str], str, None]
    actors_names: Union[List[str], str, None]
    writers_names: Union[List[str], str, None]
    actors: List[dict]
    writers: List[dict]
