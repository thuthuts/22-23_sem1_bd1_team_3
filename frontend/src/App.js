import Navbar from './components/navbar/Navbar';
import './App.scss';
import {Routes, Route} from 'react-router-dom';
import HomeContainer from './components/containers/HomeContainer';
import * as Constants from './Constants';
import CompanyNewsContainer from './components/containers/CompanyNewsContainer';
import Top from './components/navbar/Top';

function App() {
  return (
  <>
      <Top />
      <Routes>
        <Route path={Constants.HOME_ROUTE} exact element={HomeContainer}/>
        <Route path={Constants.COMPANY_NEWS_ROUTE} exact element={CompanyNewsContainer}/>
        <Route path={Constants.BRANCH_NEWS_ROUTE} exact element={CompanyNewsContainer}/>
        <Route path={Constants.SOCIAL_MEDIA_ROUTE} exact element={CompanyNewsContainer}/>
      </Routes>
  </>
  );
}

export default App;
