import React, {useState, useEffect} from 'react';
import {useParams, useHistory} from 'react-router-dom';
import {Navbar, Form, FormControl, Button, Col, Row, Container, Table} from 'react-bootstrap';


function App() {
  let { id } = useParams();


  const history = useHistory();
  const [userId, setUserId] = useState(id);
  const [data, setData] = useState();

  useEffect(() => {
    fetch(`http://178.154.240.169:5000/get_recommendations_mock/${userId}`)
    .then(res => res.json())
    .then(res => setData(res))
  }, [id]);
  


  const handleChange = (event) => {
    setUserId(event.target.value);
  }

  const handleSubmit = (event, userId) => {
    event.preventDefault();
    history.push(`/users/${userId}`);
  }

  const table_rec_header = (
    <tr>
      <th>rank</th>
      <th>id</th>
      <th>author</th>
      <th>title</th>
    </tr>
  )
  const table_rec_content = data?.recommendations?.map(({id, title, author, rank}) => {
    return (
      <tr>
        <td>{rank}</td>
        <td>{id}</td>
        <td>{author}</td>
        <td>{title}</td>
      </tr>
    )
  })
  const table_rec = (
    <Table striped bordered hover>
      <thead>
        {table_rec_header}
      </thead>
      <tbody>
        {table_rec_content}
      </tbody>
    </Table>
  )

  const table_hist_header = (
    <tr>
      <th>id</th>
      <th>author</th>
      <th>title</th>
    </tr>
  )
  const table_hist_content = data?.history?.map(({id, title, author}) => {
    return (
      <tr>
        <td>{id}</td>
        <td>{author}</td>
        <td>{title}</td>
      </tr>
    )
  })
  const table_hist = (
    <Table striped bordered hover>
      <thead>
        {table_hist_header}
      </thead>
      <tbody>
        {table_hist_content}
      </tbody>
    </Table>
  )



  return (
    <div className="App">
      <Container>
        <Col> 
          <Row>
            <Navbar bg="light" expand="lg">
              <Navbar.Brand href="#">Введите id пользователя</Navbar.Brand>
              <Form className="d-flex" onChange={handleChange} onSubmit={(event) => handleSubmit(event, userId)}>
                <FormControl
                  type="search"
                  placeholder="user_id"
                  className="mr-2"
                  aria-label="Search"
                />
                <Button variant="outline-success" type="submit"> Search </Button>
              </Form>
            </Navbar>
            {/* {JSON.stringify(data)}  */}
          </Row>
          <Row>
            <Col>
              <h1> Рекомендации </h1>
              {table_rec}
            </Col>
            <Col>
              <h1> История </h1>
              {table_hist}
            </Col>

          </Row>
        </Col>
      </Container>
    </div>
  );
}

export default App;
