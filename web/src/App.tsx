import { Navigate, Route, Routes } from "react-router"

import { AppLayout } from "@/routes/app-layout"
import { AclsPage } from "@/routes/acls"
import { ConsumerGroupPage } from "@/routes/group-detail"
import { ConsumerGroupsPage } from "@/routes/groups"
import { HomePage } from "@/routes/home"
import { LoginPage } from "@/routes/login"
import { NodePage } from "@/routes/node-detail"
import { NodesPage } from "@/routes/nodes"
import { NotFoundPage } from "@/routes/not-found"
import { SchemasPage } from "@/routes/schemas"
import { TopicPage } from "@/routes/topic-detail"
import { TopicsPage } from "@/routes/topics"

function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/login" element={<LoginPage />} />
      <Route path="/cluster/:cluster" element={<AppLayout />}>
        <Route index element={<Navigate to="topics" replace />} />
        <Route path="nodes" element={<NodesPage />} />
        <Route path="nodes/:id" element={<NodePage />} />
        <Route path="topics" element={<TopicsPage />} />
        <Route path="topics/:topic" element={<TopicPage />} />
        <Route path="groups" element={<ConsumerGroupsPage />} />
        <Route path="groups/:group" element={<ConsumerGroupPage />} />
        <Route path="schemas" element={<SchemasPage />} />
        <Route path="acls" element={<AclsPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
      <Route path="*" element={<NotFoundPage />} />
    </Routes>
  )
}

export default App
