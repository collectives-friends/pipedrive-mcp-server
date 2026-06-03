import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import * as pipedrive from "pipedrive";
import * as dotenv from 'dotenv';
import Bottleneck from 'bottleneck';
import jwt from 'jsonwebtoken';
import http from 'http';
import crypto from 'crypto';

// Type for error handling
interface ErrorWithMessage {
  message: string;
}

function isErrorWithMessage(error: unknown): error is ErrorWithMessage {
  return (
    typeof error === 'object' &&
    error !== null &&
    'message' in error &&
    typeof (error as Record<string, unknown>).message === 'string'
  );
}

function getErrorMessage(error: unknown): string {
  if (isErrorWithMessage(error)) {
    return error.message;
  }
  return String(error);
}

// Load environment variables
dotenv.config();

// Check for required environment variables
if (!process.env.PIPEDRIVE_API_TOKEN) {
  console.error("ERROR: PIPEDRIVE_API_TOKEN environment variable is required");
  process.exit(1);
}

if (!process.env.PIPEDRIVE_DOMAIN) {
  console.error("ERROR: PIPEDRIVE_DOMAIN environment variable is required (e.g., 'ukkofi.pipedrive.com')");
  process.exit(1);
}

const jwtSecret = process.env.MCP_JWT_SECRET;
const jwtAlgorithm = (process.env.MCP_JWT_ALGORITHM || 'HS256') as jwt.Algorithm;
const jwtVerifyOptions = {
  algorithms: [jwtAlgorithm],
  audience: process.env.MCP_JWT_AUDIENCE,
  issuer: process.env.MCP_JWT_ISSUER,
};

if (jwtSecret) {
  const bootToken = process.env.MCP_JWT_TOKEN;
  if (!bootToken) {
    console.error("ERROR: MCP_JWT_TOKEN environment variable is required when MCP_JWT_SECRET is set");
    process.exit(1);
  }

  try {
    jwt.verify(bootToken, jwtSecret, jwtVerifyOptions);
  } catch (error) {
    console.error("ERROR: Failed to verify MCP_JWT_TOKEN", error);
    process.exit(1);
  }
}

const verifyRequestAuthentication = (req: http.IncomingMessage) => {
  if (!jwtSecret) {
    return { ok: true } as const;
  }

  const header = req.headers['authorization'];
  if (!header) {
    return { ok: false, status: 401, message: 'Missing Authorization header' } as const;
  }

  const [scheme, token] = header.split(' ');
  if (scheme !== 'Bearer' || !token) {
    return { ok: false, status: 401, message: 'Invalid Authorization header format' } as const;
  }

  try {
    jwt.verify(token, jwtSecret, jwtVerifyOptions);
    return { ok: true } as const;
  } catch (error) {
    return { ok: false, status: 401, message: 'Invalid or expired token' } as const;
  }
};

const limiter = new Bottleneck({
  minTime: Number(process.env.PIPEDRIVE_RATE_LIMIT_MIN_TIME_MS || 250),
  maxConcurrent: Number(process.env.PIPEDRIVE_RATE_LIMIT_MAX_CONCURRENT || 2),
});

const withRateLimit = <T extends object>(client: T): T => {
  return new Proxy(client, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value === 'function') {
        return (...args: unknown[]) => limiter.schedule(() => (value as Function).apply(target, args));
      }
      return value;
    },
  });
};

// Initialize Pipedrive API client with API token and custom domain
const apiClient = new pipedrive.ApiClient();
apiClient.basePath = `https://${process.env.PIPEDRIVE_DOMAIN}/api/v1`;
apiClient.authentications = apiClient.authentications || {};
apiClient.authentications['api_key'] = {
  type: 'apiKey',
  'in': 'query',
  name: 'api_token',
  apiKey: process.env.PIPEDRIVE_API_TOKEN
};

// Initialize Pipedrive API clients
const dealsApi = withRateLimit(new pipedrive.DealsApi(apiClient));
const personsApi = withRateLimit(new pipedrive.PersonsApi(apiClient));
const organizationsApi = withRateLimit(new pipedrive.OrganizationsApi(apiClient));
const pipelinesApi = withRateLimit(new pipedrive.PipelinesApi(apiClient));
// @ts-ignore - StagesApi exists at runtime but may not be in type definitions
const stagesApi = withRateLimit(new pipedrive.StagesApi(apiClient));
const itemSearchApi = withRateLimit(new pipedrive.ItemSearchApi(apiClient));
const leadsApi = withRateLimit(new pipedrive.LeadsApi(apiClient));
// @ts-ignore - ActivitiesApi exists but may not be in type definitions
const activitiesApi = withRateLimit(new pipedrive.ActivitiesApi(apiClient));
// @ts-ignore - NotesApi exists but may not be in type definitions
const notesApi = withRateLimit(new pipedrive.NotesApi(apiClient));
// @ts-ignore - UsersApi exists but may not be in type definitions
const usersApi = withRateLimit(new pipedrive.UsersApi(apiClient));

// Factory to create a fully configured MCP server instance.
// A new instance is needed per transport connection (SDK requirement).
function createServer(): McpServer {

const server = new McpServer({
  name: "pipedrive-mcp-server",
  version: "1.0.4",
});

// === TOOLS ===

// Get all users (for finding owner IDs)
server.tool(
  "get-users",
  "Get all users/owners from Pipedrive to identify owner IDs for filtering deals",
  {},
  async () => {
    try {
      const response = await usersApi.getUsers();
      const users = response.data?.map((user: any) => ({
        id: user.id,
        name: user.name,
        email: user.email,
        active_flag: user.active_flag,
        role_name: user.role_name
      })) || [];

      return {
        content: [{
          type: "text",
          text: JSON.stringify({
            summary: `Found ${users.length} users in your Pipedrive account`,
            users: users
          }, null, 2)
        }]
      };
    } catch (error) {
      console.error("Error fetching users:", error);
      return {
        content: [{
          type: "text",
          text: `Error fetching users: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get deals with flexible filtering options
// @ts-ignore - Type instantiation depth issue with complex zod schema
server.tool(
  "get-deals",
  "Get deals from Pipedrive with flexible filtering options including search by title, date range, owner, stage, status, and more. Use 'get-users' tool first to find owner IDs.",
  {
    searchTitle: z.string().optional().describe("Search deals by title/name (partial matches supported)"),
    daysBack: z.number().optional().describe("Number of days back to fetch deals based on last activity date (default: 365)"),
    ownerId: z.number().optional().describe("Filter deals by owner/user ID (use get-users tool to find IDs)"),
    stageId: z.number().optional().describe("Filter deals by stage ID"),
    status: z.enum(['open', 'won', 'lost', 'deleted']).optional().describe("Filter deals by status (default: open)"),
    pipelineId: z.number().optional().describe("Filter deals by pipeline ID"),
    minValue: z.number().optional().describe("Minimum deal value filter"),
    maxValue: z.number().optional().describe("Maximum deal value filter"),
    limit: z.number().optional().describe("Maximum number of deals to return (default: 500)")
  },
  async ({
    searchTitle,
    daysBack = 365,
    ownerId,
    stageId,
    status = 'open',
    pipelineId,
    minValue,
    maxValue,
    limit = 500
  }) => {
    try {
      let filteredDeals: any[] = [];

      // If searching by title, use the search API first
      if (searchTitle) {
        // @ts-ignore - Bypass incorrect TypeScript definition
        const searchResponse = await dealsApi.searchDeals(searchTitle);
        filteredDeals = searchResponse.data || [];
      } else {
        // Calculate the date filter (daysBack days ago)
        const filterDate = new Date();
        filterDate.setDate(filterDate.getDate() - daysBack);
        const startDate = filterDate.toISOString().split('T')[0]; // Format as YYYY-MM-DD

        // Build API parameters (using actual Pipedrive API parameter names)
        const params: any = {
          sort: 'last_activity_date DESC',
          status: status,
          limit: limit
        };

        // Add optional filters
        if (ownerId) params.user_id = ownerId;
        if (stageId) params.stage_id = stageId;
        if (pipelineId) params.pipeline_id = pipelineId;

        // Fetch deals with filters
        // @ts-ignore - getDeals accepts parameters but types may be incomplete
        const response = await dealsApi.getDeals(params);
        filteredDeals = response.data || [];
      }

      // Apply additional client-side filtering

      // Filter by date if not searching by title
      if (!searchTitle) {
        const filterDate = new Date();
        filterDate.setDate(filterDate.getDate() - daysBack);

        filteredDeals = filteredDeals.filter((deal: any) => {
          if (!deal.last_activity_date) return false;
          const dealActivityDate = new Date(deal.last_activity_date);
          return dealActivityDate >= filterDate;
        });
      }

      // Filter by owner if specified and not already applied in API call
      if (ownerId && searchTitle) {
        filteredDeals = filteredDeals.filter((deal: any) => deal.owner_id === ownerId);
      }

      // Filter by status if specified and searching by title
      if (status && searchTitle) {
        filteredDeals = filteredDeals.filter((deal: any) => deal.status === status);
      }

      // Filter by stage if specified and not already applied in API call
      if (stageId && (searchTitle || !stageId)) {
        filteredDeals = filteredDeals.filter((deal: any) => deal.stage_id === stageId);
      }

      // Filter by pipeline if specified and not already applied in API call
      if (pipelineId && (searchTitle || !pipelineId)) {
        filteredDeals = filteredDeals.filter((deal: any) => deal.pipeline_id === pipelineId);
      }

      // Filter by value range if specified
      if (minValue !== undefined || maxValue !== undefined) {
        filteredDeals = filteredDeals.filter((deal: any) => {
          const value = parseFloat(deal.value) || 0;
          if (minValue !== undefined && value < minValue) return false;
          if (maxValue !== undefined && value > maxValue) return false;
          return true;
        });
      }

      // Apply limit
      if (filteredDeals.length > limit) {
        filteredDeals = filteredDeals.slice(0, limit);
      }

      // Build filter summary for response
      const filterSummary = {
        ...(searchTitle && { search_title: searchTitle }),
        ...(!searchTitle && { days_back: daysBack }),
        ...(!searchTitle && { filter_date: new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000).toISOString().split('T')[0] }),
        status: status,
        ...(ownerId && { owner_id: ownerId }),
        ...(stageId && { stage_id: stageId }),
        ...(pipelineId && { pipeline_id: pipelineId }),
        ...(minValue !== undefined && { min_value: minValue }),
        ...(maxValue !== undefined && { max_value: maxValue }),
        total_deals_found: filteredDeals.length,
        limit_applied: limit
      };

      // Summarize deals to avoid massive responses but include notes and booking details
      const bookingFieldKey = "8f4b27fbd9dfc70d2296f23ce76987051ad7324e";
      const summarizedDeals = filteredDeals.map((deal: any) => ({
        id: deal.id,
        title: deal.title,
        value: deal.value,
        currency: deal.currency,
        status: deal.status,
        stage_name: deal.stage?.name || 'Unknown',
        pipeline_name: deal.pipeline?.name || 'Unknown',
        owner_name: deal.owner?.name || 'Unknown',
        organization_name: deal.org?.name || null,
        person_name: deal.person?.name || null,
        add_time: deal.add_time,
        last_activity_date: deal.last_activity_date,
        close_time: deal.close_time,
        won_time: deal.won_time,
        lost_time: deal.lost_time,
        notes_count: deal.notes_count || 0,
        // Include recent notes if available
        notes: deal.notes || [],
        // Include custom booking details field
        booking_details: deal[bookingFieldKey] || null
      }));

      return {
        content: [{
          type: "text",
          text: JSON.stringify({
            summary: searchTitle
              ? `Found ${filteredDeals.length} deals matching title search "${searchTitle}"`
              : `Found ${filteredDeals.length} deals matching the specified filters`,
            filters_applied: filterSummary,
            total_found: filteredDeals.length,
            deals: summarizedDeals.slice(0, 30) // Limit to 30 deals max to prevent huge responses
          }, null, 2)
        }]
      };
    } catch (error) {
      console.error("Error fetching deals:", error);
      return {
        content: [{
          type: "text",
          text: `Error fetching deals: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get deal by ID
server.tool(
  "get-deal",
  "Get a specific deal by ID including custom fields",
  {
    dealId: z.number().describe("Pipedrive deal ID")
  },
  async ({ dealId }) => {
    try {
      // @ts-ignore - Bypass incorrect TypeScript definition, API expects just the ID
      const response = await dealsApi.getDeal(dealId);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error fetching deal ${dealId}:`, error);
      return {
        content: [{
          type: "text",
          text: `Error fetching deal ${dealId}: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get deal notes and custom booking details
server.tool(
  "get-deal-notes",
  "Get detailed notes and custom booking details for a specific deal",
  {
    dealId: z.number().describe("Pipedrive deal ID"),
    limit: z.number().optional().describe("Maximum number of notes to return (default: 20)"),
    start: z.number().optional().describe("Pagination start index (default: 0). Use with limit to paginate through notes.")
  },
  async ({ dealId, limit = 20, start = 0 }) => {
    try {
      const result: any = {
        deal_id: dealId,
        notes: [],
        booking_details: null
      };

      // Get deal details including custom fields
      try {
        // @ts-ignore - Bypass incorrect TypeScript definition
        const dealResponse = await dealsApi.getDeal(dealId);
        const deal = dealResponse.data;

        // Extract custom booking field
        const bookingFieldKey = "8f4b27fbd9dfc70d2296f23ce76987051ad7324e";
        if (deal && deal[bookingFieldKey]) {
          result.booking_details = deal[bookingFieldKey];
        }
      } catch (dealError) {
        console.error(`Error fetching deal details for ${dealId}:`, dealError);
        result.deal_error = getErrorMessage(dealError);
      }

      // Get deal notes
      try {
        // @ts-ignore - API parameters may not be fully typed
        // @ts-ignore - Bypass incorrect TypeScript definition
        const notesResponse = await notesApi.getNotes({
          deal_id: dealId,
          limit: limit,
          start: start
        });
        result.notes = notesResponse.data || [];
      } catch (noteError) {
        console.error(`Error fetching notes for deal ${dealId}:`, noteError);
        result.notes_error = getErrorMessage(noteError);
      }

      return {
        content: [{
          type: "text",
          text: JSON.stringify({
            summary: `Retrieved ${result.notes.length} notes for deal ${dealId} (start: ${start}, limit: ${limit})`,
            pagination: {
              start: start,
              limit: limit,
              returned: result.notes.length,
              has_more: result.notes.length === limit
            },
            ...result
          }, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error fetching deal notes ${dealId}:`, error);
      return {
        content: [{
          type: "text",
          text: `Error fetching deal notes ${dealId}: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Search deals
// @ts-ignore - Type instantiation depth issue with complex zod schema
server.tool(
  "search-deals",
  "Search deals by term",
  {
    term: z.string().describe("Search term for deals")
  },
  async ({ term }) => {
    try {
      // @ts-ignore - Bypass incorrect TypeScript definition
      const response = await dealsApi.searchDeals(term);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error searching deals with term "${term}":`, error);
      return {
        content: [{
          type: "text",
          text: `Error searching deals: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get all persons
server.tool(
  "get-persons",
  "Get all persons from Pipedrive including custom fields",
  {},
  async () => {
    try {
      const response = await personsApi.getPersons();
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error("Error fetching persons:", error);
      return {
        content: [{
          type: "text",
          text: `Error fetching persons: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get person by ID
server.tool(
  "get-person",
  "Get a specific person by ID including custom fields",
  {
    personId: z.number().describe("Pipedrive person ID")
  },
  async ({ personId }) => {
    try {
      // @ts-ignore - Bypass incorrect TypeScript definition
      const response = await personsApi.getPerson(personId);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error fetching person ${personId}:`, error);
      return {
        content: [{
          type: "text",
          text: `Error fetching person ${personId}: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Search persons
server.tool(
  "search-persons",
  "Search persons by term",
  {
    term: z.string().describe("Search term for persons")
  },
  async ({ term }) => {
    try {
      // @ts-ignore - Bypass incorrect TypeScript definition
      const response = await personsApi.searchPersons(term);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error searching persons with term "${term}":`, error);
      return {
        content: [{
          type: "text",
          text: `Error searching persons: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get all organizations
server.tool(
  "get-organizations",
  "Get all organizations from Pipedrive including custom fields",
  {},
  async () => {
    try {
      const response = await organizationsApi.getOrganizations();
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error("Error fetching organizations:", error);
      return {
        content: [{
          type: "text",
          text: `Error fetching organizations: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get organization by ID
server.tool(
  "get-organization",
  "Get a specific organization by ID including custom fields",
  {
    organizationId: z.number().describe("Pipedrive organization ID")
  },
  async ({ organizationId }) => {
    try {
      // @ts-ignore - Bypass incorrect TypeScript definition
      const response = await organizationsApi.getOrganization(organizationId);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error fetching organization ${organizationId}:`, error);
      return {
        content: [{
          type: "text",
          text: `Error fetching organization ${organizationId}: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Search organizations
server.tool(
  "search-organizations",
  "Search organizations by term",
  {
    term: z.string().describe("Search term for organizations")
  },
  async ({ term }) => {
    try {
      // @ts-ignore - API method exists but TypeScript definition is wrong
      const response = await (organizationsApi as any).searchOrganization({ term });
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error searching organizations with term "${term}":`, error);
      return {
        content: [{
          type: "text",
          text: `Error searching organizations: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get all pipelines
server.tool(
  "get-pipelines",
  "Get all pipelines from Pipedrive",
  {},
  async () => {
    try {
      const response = await pipelinesApi.getPipelines();
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error("Error fetching pipelines:", error);
      return {
        content: [{
          type: "text",
          text: `Error fetching pipelines: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get pipeline by ID
server.tool(
  "get-pipeline",
  "Get a specific pipeline by ID",
  {
    pipelineId: z.number().describe("Pipedrive pipeline ID")
  },
  async ({ pipelineId }) => {
    try {
      // @ts-ignore - Bypass incorrect TypeScript definition
      const response = await pipelinesApi.getPipeline(pipelineId);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error fetching pipeline ${pipelineId}:`, error);
      return {
        content: [{
          type: "text",
          text: `Error fetching pipeline ${pipelineId}: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Get all stages
server.tool(
  "get-stages",
  "Get all stages from Pipedrive",
  {},
  async () => {
    try {
      // Since the stages are related to pipelines, we'll get all pipelines first
      const pipelinesResponse = await pipelinesApi.getPipelines();
      const pipelines = pipelinesResponse.data || [];
      
      // For each pipeline, fetch its stages
      const allStages = [];
      for (const pipeline of pipelines) {
        try {
          const stagesResponse = await stagesApi.getStages({ pipeline_id: pipeline.id });
          const stagesData = Array.isArray(stagesResponse?.data)
            ? stagesResponse.data
            : [];

          if (stagesData.length > 0) {
            const pipelineStages = stagesData.map((stage: any) => ({
              ...stage,
              pipeline_name: pipeline.name
            }));
            allStages.push(...pipelineStages);
          }
        } catch (e) {
          console.error(`Error fetching stages for pipeline ${pipeline.id}:`, e);
        }
      }
      
      return {
        content: [{
          type: "text",
          text: JSON.stringify(allStages, null, 2)
        }]
      };
    } catch (error) {
      console.error("Error fetching stages:", error);
      return {
        content: [{
          type: "text",
          text: `Error fetching stages: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Search leads
server.tool(
  "search-leads",
  "Search leads by term",
  {
    term: z.string().describe("Search term for leads")
  },
  async ({ term }) => {
    try {
      // @ts-ignore - Bypass incorrect TypeScript definition
      const response = await leadsApi.searchLeads(term);
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error searching leads with term "${term}":`, error);
      return {
        content: [{
          type: "text",
          text: `Error searching leads: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// Generic search across item types
server.tool(
  "search-all",
  "Search across all item types (deals, persons, organizations, etc.)",
  {
    term: z.string().describe("Search term"),
    itemTypes: z.string().optional().describe("Comma-separated list of item types to search (deal,person,organization,product,file,activity,lead)")
  },
  async ({ term, itemTypes }) => {
    try {
      const itemType = itemTypes; // Just rename the parameter
      const response = await itemSearchApi.searchItem({ 
        term,
        itemType 
      });
      return {
        content: [{
          type: "text",
          text: JSON.stringify(response.data, null, 2)
        }]
      };
    } catch (error) {
      console.error(`Error performing search with term "${term}":`, error);
      return {
        content: [{
          type: "text",
          text: `Error performing search: ${getErrorMessage(error)}`
        }],
        isError: true
      };
    }
  }
);

// === WRITE TOOLS ===
// Registered via a loosely-typed handle: the MCP SDK's server.tool() generic
// inference is O(n) deep in the number of tools and blows past the build
// container's memory (TS2589 / tsc SIGABRT) once the write tools are added.
// Casting to a loose type skips that inference. Arg shapes are runtime-verified.
const writeServer = server as unknown as { tool: (...args: any[]) => void };

// Create a new deal
writeServer.tool(
  "create-deal",
  "Create a new deal in Pipedrive. Use get-users for ownerId, get-pipelines/get-stages for pipeline/stage IDs, and get-persons/get-organizations to link a contact.",
  {
    title: z.string().describe("Deal title (required)"),
    value: z.number().optional().describe("Deal value (number)"),
    currency: z.string().optional().describe("Currency code, e.g. DKK, EUR, USD"),
    personId: z.number().optional().describe("Linked person ID"),
    orgId: z.number().optional().describe("Linked organization ID"),
    pipelineId: z.number().optional().describe("Pipeline ID"),
    stageId: z.number().optional().describe("Stage ID"),
    status: z.enum(['open', 'won', 'lost']).optional().describe("Deal status (default: open)"),
    ownerId: z.number().optional().describe("Owner/user ID"),
    expectedCloseDate: z.string().optional().describe("Expected close date (YYYY-MM-DD)"),
    visibleTo: z.number().optional().describe("Visibility (1=owner, 3=entire company)")
  },
  async ({ title, value, currency, personId, orgId, pipelineId, stageId, status, ownerId, expectedCloseDate, visibleTo }: any) => {
    try {
      const newDeal: any = { title };
      if (value !== undefined) newDeal.value = value;
      if (currency !== undefined) newDeal.currency = currency;
      if (personId !== undefined) newDeal.person_id = personId;
      if (orgId !== undefined) newDeal.org_id = orgId;
      if (pipelineId !== undefined) newDeal.pipeline_id = pipelineId;
      if (stageId !== undefined) newDeal.stage_id = stageId;
      if (status !== undefined) newDeal.status = status;
      if (ownerId !== undefined) newDeal.user_id = ownerId;
      if (expectedCloseDate !== undefined) newDeal.expected_close_date = expectedCloseDate;
      if (visibleTo !== undefined) newDeal.visible_to = visibleTo;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await dealsApi.addDeal(newDeal);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error("Error creating deal:", error);
      return { content: [{ type: "text", text: `Error creating deal: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Update an existing deal (incl. move stage, change value/status/owner)
writeServer.tool(
  "update-deal",
  "Update an existing deal by ID. Use this to move a deal to another stage, change value, mark won/lost, reassign owner, or relink person/organization. Only the fields you provide are changed.",
  {
    dealId: z.number().describe("Pipedrive deal ID (required)"),
    title: z.string().optional().describe("New deal title"),
    value: z.number().optional().describe("New deal value"),
    currency: z.string().optional().describe("Currency code"),
    personId: z.number().optional().describe("Linked person ID"),
    orgId: z.number().optional().describe("Linked organization ID"),
    pipelineId: z.number().optional().describe("Pipeline ID"),
    stageId: z.number().optional().describe("Stage ID (move the deal to this stage)"),
    status: z.enum(['open', 'won', 'lost', 'deleted']).optional().describe("Deal status"),
    ownerId: z.number().optional().describe("Owner/user ID"),
    expectedCloseDate: z.string().optional().describe("Expected close date (YYYY-MM-DD)"),
    visibleTo: z.number().optional().describe("Visibility (1=owner, 3=entire company)")
  },
  async ({ dealId, title, value, currency, personId, orgId, pipelineId, stageId, status, ownerId, expectedCloseDate, visibleTo }: any) => {
    try {
      const updateDealRequest: any = {};
      if (title !== undefined) updateDealRequest.title = title;
      if (value !== undefined) updateDealRequest.value = value;
      if (currency !== undefined) updateDealRequest.currency = currency;
      if (personId !== undefined) updateDealRequest.person_id = personId;
      if (orgId !== undefined) updateDealRequest.org_id = orgId;
      if (pipelineId !== undefined) updateDealRequest.pipeline_id = pipelineId;
      if (stageId !== undefined) updateDealRequest.stage_id = stageId;
      if (status !== undefined) updateDealRequest.status = status;
      if (ownerId !== undefined) updateDealRequest.user_id = ownerId;
      if (expectedCloseDate !== undefined) updateDealRequest.expected_close_date = expectedCloseDate;
      if (visibleTo !== undefined) updateDealRequest.visible_to = visibleTo;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await dealsApi.updateDeal(dealId, updateDealRequest);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error(`Error updating deal ${dealId}:`, error);
      return { content: [{ type: "text", text: `Error updating deal ${dealId}: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Create a new person
writeServer.tool(
  "create-person",
  "Create a new person (contact) in Pipedrive. Email and phone are optional single values.",
  {
    name: z.string().describe("Person name (required)"),
    email: z.string().optional().describe("Email address"),
    phone: z.string().optional().describe("Phone number"),
    orgId: z.number().optional().describe("Linked organization ID"),
    ownerId: z.number().optional().describe("Owner/user ID"),
    visibleTo: z.number().optional().describe("Visibility (1=owner, 3=entire company)")
  },
  async ({ name, email, phone, orgId, ownerId, visibleTo }: any) => {
    try {
      const newPerson: any = { name };
      if (email !== undefined) newPerson.email = [{ value: email, primary: true }];
      if (phone !== undefined) newPerson.phone = [{ value: phone, primary: true }];
      if (orgId !== undefined) newPerson.org_id = orgId;
      if (ownerId !== undefined) newPerson.owner_id = ownerId;
      if (visibleTo !== undefined) newPerson.visible_to = visibleTo;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await personsApi.addPerson(newPerson);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error("Error creating person:", error);
      return { content: [{ type: "text", text: `Error creating person: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Update an existing person
writeServer.tool(
  "update-person",
  "Update an existing person by ID. Only the fields you provide are changed. Setting email/phone replaces the primary value.",
  {
    personId: z.number().describe("Pipedrive person ID (required)"),
    name: z.string().optional().describe("New name"),
    email: z.string().optional().describe("Email address (replaces primary)"),
    phone: z.string().optional().describe("Phone number (replaces primary)"),
    orgId: z.number().optional().describe("Linked organization ID"),
    ownerId: z.number().optional().describe("Owner/user ID"),
    visibleTo: z.number().optional().describe("Visibility (1=owner, 3=entire company)")
  },
  async ({ personId, name, email, phone, orgId, ownerId, visibleTo }: any) => {
    try {
      const updatePerson: any = {};
      if (name !== undefined) updatePerson.name = name;
      if (email !== undefined) updatePerson.email = [{ value: email, primary: true }];
      if (phone !== undefined) updatePerson.phone = [{ value: phone, primary: true }];
      if (orgId !== undefined) updatePerson.org_id = orgId;
      if (ownerId !== undefined) updatePerson.owner_id = ownerId;
      if (visibleTo !== undefined) updatePerson.visible_to = visibleTo;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await personsApi.updatePerson(personId, updatePerson);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error(`Error updating person ${personId}:`, error);
      return { content: [{ type: "text", text: `Error updating person ${personId}: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Create a new organization
writeServer.tool(
  "create-organization",
  "Create a new organization in Pipedrive.",
  {
    name: z.string().describe("Organization name (required)"),
    ownerId: z.number().optional().describe("Owner/user ID"),
    visibleTo: z.number().optional().describe("Visibility (1=owner, 3=entire company)")
  },
  async ({ name, ownerId, visibleTo }: any) => {
    try {
      const newOrganization: any = { name };
      if (ownerId !== undefined) newOrganization.owner_id = ownerId;
      if (visibleTo !== undefined) newOrganization.visible_to = visibleTo;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await organizationsApi.addOrganization(newOrganization);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error("Error creating organization:", error);
      return { content: [{ type: "text", text: `Error creating organization: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Update an existing organization
writeServer.tool(
  "update-organization",
  "Update an existing organization by ID. Only the fields you provide are changed.",
  {
    orgId: z.number().describe("Pipedrive organization ID (required)"),
    name: z.string().optional().describe("New name"),
    ownerId: z.number().optional().describe("Owner/user ID"),
    visibleTo: z.number().optional().describe("Visibility (1=owner, 3=entire company)")
  },
  async ({ orgId, name, ownerId, visibleTo }: any) => {
    try {
      const updateOrganization: any = {};
      if (name !== undefined) updateOrganization.name = name;
      if (ownerId !== undefined) updateOrganization.owner_id = ownerId;
      if (visibleTo !== undefined) updateOrganization.visible_to = visibleTo;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await organizationsApi.updateOrganization(orgId, updateOrganization);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error(`Error updating organization ${orgId}:`, error);
      return { content: [{ type: "text", text: `Error updating organization ${orgId}: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Add a note (to a deal, person, or organization)
writeServer.tool(
  "add-note",
  "Add a note in Pipedrive. Provide at least one of dealId, personId, or orgId to attach the note. Content supports basic HTML.",
  {
    content: z.string().describe("Note content (required)"),
    dealId: z.number().optional().describe("Attach to this deal ID"),
    personId: z.number().optional().describe("Attach to this person ID"),
    orgId: z.number().optional().describe("Attach to this organization ID")
  },
  async ({ content, dealId, personId, orgId }: any) => {
    try {
      if (dealId === undefined && personId === undefined && orgId === undefined) {
        return { content: [{ type: "text", text: "Error: provide at least one of dealId, personId, or orgId to attach the note." }], isError: true };
      }
      const addNoteRequest: any = { content };
      if (dealId !== undefined) addNoteRequest.deal_id = dealId;
      if (personId !== undefined) addNoteRequest.person_id = personId;
      if (orgId !== undefined) addNoteRequest.org_id = orgId;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await notesApi.addNote(addNoteRequest);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error("Error adding note:", error);
      return { content: [{ type: "text", text: `Error adding note: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Create a new activity (task/call/meeting)
writeServer.tool(
  "create-activity",
  "Create a new activity (task, call, meeting, etc.) in Pipedrive. Link it to a deal/person/organization via the optional IDs.",
  {
    subject: z.string().describe("Activity subject/title (required)"),
    type: z.string().optional().describe("Activity type key, e.g. call, meeting, task, email, deadline (default: task)"),
    dueDate: z.string().optional().describe("Due date (YYYY-MM-DD)"),
    dueTime: z.string().optional().describe("Due time (HH:MM)"),
    duration: z.string().optional().describe("Duration (HH:MM)"),
    note: z.string().optional().describe("Note/description for the activity"),
    done: z.boolean().optional().describe("Mark as done (default: false)"),
    dealId: z.number().optional().describe("Linked deal ID"),
    personId: z.number().optional().describe("Linked person ID"),
    orgId: z.number().optional().describe("Linked organization ID"),
    ownerId: z.number().optional().describe("Owner/user ID")
  },
  async ({ subject, type, dueDate, dueTime, duration, note, done, dealId, personId, orgId, ownerId }: any) => {
    try {
      const activityPostObject: any = { subject, type: type ?? 'task' };
      if (dueDate !== undefined) activityPostObject.due_date = dueDate;
      if (dueTime !== undefined) activityPostObject.due_time = dueTime;
      if (duration !== undefined) activityPostObject.duration = duration;
      if (note !== undefined) activityPostObject.note = note;
      if (done !== undefined) activityPostObject.done = done ? 1 : 0;
      if (dealId !== undefined) activityPostObject.deal_id = dealId;
      if (personId !== undefined) activityPostObject.person_id = personId;
      if (orgId !== undefined) activityPostObject.org_id = orgId;
      if (ownerId !== undefined) activityPostObject.owner_id = ownerId;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await activitiesApi.addActivity(activityPostObject);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error("Error creating activity:", error);
      return { content: [{ type: "text", text: `Error creating activity: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// Update an existing activity (e.g. mark done)
writeServer.tool(
  "update-activity",
  "Update an existing activity by ID. Common use: mark an activity done. Only the fields you provide are changed.",
  {
    activityId: z.number().describe("Pipedrive activity ID (required)"),
    subject: z.string().optional().describe("New subject"),
    type: z.string().optional().describe("Activity type key"),
    dueDate: z.string().optional().describe("Due date (YYYY-MM-DD)"),
    dueTime: z.string().optional().describe("Due time (HH:MM)"),
    note: z.string().optional().describe("Note/description"),
    done: z.boolean().optional().describe("Mark as done/not done"),
    ownerId: z.number().optional().describe("Owner/user ID")
  },
  async ({ activityId, subject, type, dueDate, dueTime, note, done, ownerId }: any) => {
    try {
      const activityPutObject: any = {};
      if (subject !== undefined) activityPutObject.subject = subject;
      if (type !== undefined) activityPutObject.type = type;
      if (dueDate !== undefined) activityPutObject.due_date = dueDate;
      if (dueTime !== undefined) activityPutObject.due_time = dueTime;
      if (note !== undefined) activityPutObject.note = note;
      if (done !== undefined) activityPutObject.done = done ? 1 : 0;
      if (ownerId !== undefined) activityPutObject.owner_id = ownerId;
      // @ts-ignore - SDK type definitions are incomplete
      const response = await activitiesApi.updateActivity(activityId, activityPutObject);
      return { content: [{ type: "text", text: JSON.stringify(response.data, null, 2) }] };
    } catch (error) {
      console.error(`Error updating activity ${activityId}:`, error);
      return { content: [{ type: "text", text: `Error updating activity ${activityId}: ${getErrorMessage(error)}` }], isError: true };
    }
  }
);

// === PROMPTS ===

// Prompt for getting all deals
server.prompt(
  "list-all-deals",
  "List all deals in Pipedrive",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please list all deals in my Pipedrive account, showing their title, value, status, and stage."
      }
    }]
  })
);

// Prompt for getting all persons
server.prompt(
  "list-all-persons",
  "List all persons in Pipedrive",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please list all persons in my Pipedrive account, showing their name, email, phone, and organization."
      }
    }]
  })
);

// Prompt for getting all pipelines
server.prompt(
  "list-all-pipelines",
  "List all pipelines in Pipedrive",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please list all pipelines in my Pipedrive account, showing their name and stages."
      }
    }]
  })
);

// Prompt for analyzing deals
server.prompt(
  "analyze-deals",
  "Analyze deals by stage",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please analyze the deals in my Pipedrive account, grouping them by stage and providing total value for each stage."
      }
    }]
  })
);

// Prompt for analyzing contacts
server.prompt(
  "analyze-contacts",
  "Analyze contacts by organization",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please analyze the persons in my Pipedrive account, grouping them by organization and providing a count for each organization."
      }
    }]
  })
);

// Prompt for analyzing leads
server.prompt(
  "analyze-leads",
  "Analyze leads by status",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please search for all leads in my Pipedrive account and group them by status."
      }
    }]
  })
);

// Prompt for pipeline comparison
server.prompt(
  "compare-pipelines",
  "Compare different pipelines and their stages",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please list all pipelines in my Pipedrive account and compare them by showing the stages in each pipeline."
      }
    }]
  })
);

// Prompt for finding high-value deals
server.prompt(
  "find-high-value-deals",
  "Find high-value deals",
  {},
  () => ({
    messages: [{
      role: "user",
      content: {
        type: "text",
        text: "Please identify the highest value deals in my Pipedrive account and provide information about which stage they're in and which person or organization they're associated with."
      }
    }]
  })
);

return server;
} // end createServer

// Get transport type from environment variable (default to stdio)
const transportType = process.env.MCP_TRANSPORT || 'stdio';

if (transportType === 'sse' || transportType === 'http') {
  // HTTP transport - supports both Streamable HTTP (/mcp) and legacy SSE (/sse + /message)
  const port = parseInt(process.env.MCP_PORT || '3000', 10);
  const sseEndpoint = process.env.MCP_ENDPOINT || '/message';

  // Store active transports by session ID
  const transports = new Map<string, SSEServerTransport | StreamableHTTPServerTransport>();

  // Parse JSON body from an IncomingMessage
  function parseJsonBody(req: http.IncomingMessage): Promise<unknown> {
    return new Promise((resolve, reject) => {
      let body = '';
      req.on('data', (chunk: Buffer) => { body += chunk.toString(); });
      req.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(e); }
      });
      req.on('error', reject);
    });
  }

  // --- Streamable HTTP handlers (/mcp) ---

  async function handleStreamablePost(req: http.IncomingMessage, res: http.ServerResponse) {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;
    let body: unknown;
    try {
      body = await parseJsonBody(req);
    } catch {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid JSON body' }));
      return;
    }

    // New session: initialize request without session ID
    if (!sessionId && isInitializeRequest(body)) {
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => crypto.randomUUID(),
        onsessioninitialized: (id) => {
          transports.set(id, transport);
          console.error(`Streamable HTTP session initialized: ${id}`);
        }
      });

      transport.onclose = () => {
        if (transport.sessionId) {
          console.error(`Streamable HTTP session closed: ${transport.sessionId}`);
          transports.delete(transport.sessionId);
        }
      };

      const mcpServer = createServer();
      await mcpServer.connect(transport);
      await transport.handleRequest(req, res, body);
      return;
    }

    // Existing session
    if (sessionId) {
      const transport = transports.get(sessionId);
      if (!transport || !(transport instanceof StreamableHTTPServerTransport)) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Session not found' }));
        return;
      }
      await transport.handleRequest(req, res, body);
      return;
    }

    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Missing mcp-session-id header or not an initialization request' }));
  }

  async function handleStreamableGet(req: http.IncomingMessage, res: http.ServerResponse) {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;
    if (!sessionId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing mcp-session-id header' }));
      return;
    }

    const transport = transports.get(sessionId);
    if (!transport || !(transport instanceof StreamableHTTPServerTransport)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Session not found' }));
      return;
    }

    await transport.handleRequest(req, res);
  }

  async function handleStreamableDelete(req: http.IncomingMessage, res: http.ServerResponse) {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;
    if (!sessionId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing mcp-session-id header' }));
      return;
    }

    const transport = transports.get(sessionId);
    if (!transport || !(transport instanceof StreamableHTTPServerTransport)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Session not found' }));
      return;
    }

    await transport.handleRequest(req, res);
  }

  // --- Legacy SSE handlers (/sse + /message) ---

  async function handleLegacySseGet(req: http.IncomingMessage, res: http.ServerResponse) {
    console.error('New SSE connection request');
    const transport = new SSEServerTransport(sseEndpoint, res);

    transports.set(transport.sessionId, transport);

    transport.onclose = () => {
      console.error(`SSE connection closed: ${transport.sessionId}`);
      transports.delete(transport.sessionId);
    };

    try {
      const mcpServer = createServer();
      await mcpServer.connect(transport);
      console.error(`SSE connection established: ${transport.sessionId}`);
    } catch (err) {
      console.error('Failed to establish SSE connection:', err);
      transports.delete(transport.sessionId);
    }
  }

  async function handleLegacySsePost(req: http.IncomingMessage, res: http.ServerResponse) {
    const url = new URL(req.url!, `http://${req.headers.host}`);
    const sessionId = url.searchParams.get('sessionId') || req.headers['x-session-id'] as string;

    if (!sessionId) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Missing sessionId' }));
      return;
    }

    const transport = transports.get(sessionId);
    if (!transport || !(transport instanceof SSEServerTransport)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Session not found' }));
      return;
    }

    try {
      await transport.handlePostMessage(req, res);
    } catch (err) {
      console.error('Error handling POST message:', err);
      if (!res.headersSent) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Internal server error' }));
      }
    }
  }

  // --- HTTP Server ---

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url!, `http://${req.headers.host}`);

    // Enable CORS
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Session-Id, Mcp-Session-Id');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    // Auth check for all non-OPTIONS, non-health requests
    if (url.pathname !== '/health') {
      const authResult = verifyRequestAuthentication(req);
      if (!authResult.ok) {
        res.writeHead(authResult.status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: authResult.message }));
        return;
      }
    }

    try {
      // Streamable HTTP endpoint (/mcp)
      if (url.pathname === '/mcp') {
        if (req.method === 'POST') {
          await handleStreamablePost(req, res);
        } else if (req.method === 'GET') {
          await handleStreamableGet(req, res);
        } else if (req.method === 'DELETE') {
          await handleStreamableDelete(req, res);
        } else {
          res.writeHead(405);
          res.end('Method not allowed');
        }
        return;
      }

      // Legacy SSE endpoints (/sse + /message)
      if (req.method === 'GET' && url.pathname === '/sse') {
        await handleLegacySseGet(req, res);
        return;
      }

      if (req.method === 'POST' && url.pathname === sseEndpoint) {
        await handleLegacySsePost(req, res);
        return;
      }

      // Health check
      if (req.method === 'GET' && url.pathname === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', transports: ['streamable-http', 'sse'] }));
        return;
      }

      res.writeHead(404);
      res.end('Not found');
    } catch (err) {
      console.error('Unhandled error:', err);
      if (!res.headersSent) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Internal server error' }));
      }
    }
  });

  httpServer.listen(port, () => {
    console.error(`Pipedrive MCP Server listening on port ${port}`);
    console.error(`Streamable HTTP endpoint: http://localhost:${port}/mcp`);
    console.error(`Legacy SSE endpoint:      http://localhost:${port}/sse`);
    console.error(`Legacy message endpoint:  http://localhost:${port}${sseEndpoint}`);
  });
} else {
  // Default: stdio transport
  const transport = new StdioServerTransport();
  createServer().connect(transport).catch(err => {
    console.error("Failed to start MCP server:", err);
    process.exit(1);
  });

  console.error("Pipedrive MCP Server started (stdio transport)");
}
