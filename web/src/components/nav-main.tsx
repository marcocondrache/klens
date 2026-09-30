import { Link, useMatchRoute } from "@tanstack/react-router";

import {
  SidebarGroup,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { formatCount } from "@/lib/format";
import {
  clusterSectionTo,
  SECTION_GROUPS,
  type ClusterSection,
  type Section,
} from "@/lib/sections";

export function NavMain({
  cluster,
  sections,
  counts,
}: {
  cluster: string;
  sections: Section[];
  counts: Partial<Record<ClusterSection, number>>;
}) {
  const matchRoute = useMatchRoute();
  const { setOpenMobile } = useSidebar();

  return SECTION_GROUPS.map((group) => {
    const members = sections.filter((section) => section.group === group.id);
    if (members.length === 0) return null;

    return (
      <SidebarGroup key={group.id}>
        <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
        <SidebarMenu>
          {members.map((section) => {
            const to = clusterSectionTo(section.segment);
            const count = counts[section.segment];

            return (
              <SidebarMenuItem key={section.segment}>
                <SidebarMenuButton
                  isActive={Boolean(matchRoute({ to, params: { cluster }, fuzzy: true }))}
                  tooltip={section.label}
                  onClick={() => setOpenMobile(false)}
                  className="data-active:[&_svg]:text-brand"
                  render={<Link to={to} params={{ cluster }} />}
                >
                  <section.icon />
                  <span>{section.label}</span>
                </SidebarMenuButton>
                {count === undefined ? null : (
                  <SidebarMenuBadge className="numeric text-muted-foreground">
                    {formatCount(count)}
                  </SidebarMenuBadge>
                )}
              </SidebarMenuItem>
            );
          })}
        </SidebarMenu>
      </SidebarGroup>
    );
  });
}
