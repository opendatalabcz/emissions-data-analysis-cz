"use client";

import Footer from "@/components/Footer";
import Header from "@/components/Header";
import Layout, { Content } from "antd/es/layout/layout";
import theme from "./themeConfig";
import Container from "@/components/Container";
import ConfigProvider from "antd/es/config-provider";
import SearchBox from "./vehicles/SearchBox";

export default function Home() {
  return (
    <ConfigProvider theme={theme}>
      <Layout>
        <Header></Header>
        <Content className="flex flex-col items-stretch w-full mx-auto lg:w-10/12 2xl:w-8/12 mt-12">
          <Container>
            <h1 className="pb-4 text-3xl">Predikce rizikovosti vozidla</h1>
            <p className="pb-8 text-gray-600">Zadejte VIN kód vozidla (17 znaků) pro zjištění jeho rizikové třídy podle modelu strojového učení.</p>

            <SearchBox></SearchBox>
          </Container>
        </Content>
        <Footer></Footer>
      </Layout>
    </ConfigProvider>
  );
}